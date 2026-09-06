package com.resharding.worker;

import com.resharding.cluster.LeaderElectionService;
import com.resharding.config.ReshardingRootProperties;
import com.resharding.config.TableMigrationProperties;
import com.resharding.db.DataSourceLifecycleManager;
import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationStatus;
import com.resharding.domain.MigrationTask;
import com.resharding.domain.TaskCheckpoint;
import com.resharding.domain.TaskStatus;
import com.resharding.etcd.MigrationStateRepository;
import com.resharding.etcd.TaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Исполнитель Task на Worker Pod.
 *
 * <p>Алгоритм: claim → цикл batch по всему диапазону → COMPLETED.
 * Диапазон Task обрабатывается <strong>полностью</strong>: один batch не завершает
 * Task, иначе строки за пределами первого batch остались бы неперенесёнными,
 * а checkpoint уехал бы на {@code rangeTo}.
 *
 * <p>После каждого batch:
 * <ul>
 *   <li>проверяется ownership ({@code generation} + lock lease) — fencing;</li>
 *   <li>прогресс сохраняется в {@code etcd}, чтобы при переназначении Task
 *       новый Worker продолжил с последней позиции.</li>
 * </ul>
 *
 * @see BatchProcessor
 */
@Slf4j
@Service
public class WorkerExecutor {

    private final LeaderElectionService leaderElectionService;
    private final WorkerTaskClaimService claimService;
    private final BatchProcessor batchProcessor;
    private final TaskRepository taskRepository;
    private final MigrationStateRepository migrationStateRepository;
    private final TableMigrationProperties tableMigrationProperties;
    private final BatchSizeResolver batchSizeResolver;
    private final InsertGuardPolicy insertGuardPolicy;
    private final DataSourceLifecycleManager dataSourceLifecycleManager;

    public WorkerExecutor(
            LeaderElectionService leaderElectionService,
            WorkerTaskClaimService claimService,
            BatchProcessor batchProcessor,
            TaskRepository taskRepository,
            MigrationStateRepository migrationStateRepository,
            TableMigrationProperties tableMigrationProperties,
            BatchSizeResolver batchSizeResolver,
            InsertGuardPolicy insertGuardPolicy,
            DataSourceLifecycleManager dataSourceLifecycleManager) {
        this.leaderElectionService = leaderElectionService;
        this.claimService = claimService;
        this.batchProcessor = batchProcessor;
        this.taskRepository = taskRepository;
        this.migrationStateRepository = migrationStateRepository;
        this.tableMigrationProperties = tableMigrationProperties;
        this.batchSizeResolver = batchSizeResolver;
        this.insertGuardPolicy = insertGuardPolicy;
        this.dataSourceLifecycleManager = dataSourceLifecycleManager;
    }

    /** Периодически пытается захватить и выполнить Task. */
    @Scheduled(
            fixedDelayString = "${migration.scheduler-interval-ms:2000}",
            scheduler = "workerTaskScheduler")
    public void tick() {
        if (claimService.hasActiveTask()) {
            return;
        }

        Optional<MigrationState> migrationOpt = migrationStateRepository.getCurrent();
        if (migrationOpt.isEmpty() || migrationOpt.get().getStatus() != MigrationStatus.RUNNING) {
            return;
        }

        MigrationState migration = migrationOpt.get();
        if (leaderElectionService.isLeader()) {
            return;
        }

        try {
            claimService.tryClaimNext(migration.getMigrationId())
                    .ifPresent(task -> executeTask(task, migration));
        } catch (Exception e) {
            log.error("Worker tick failed for migration {}", migration.getMigrationId(), e);
            claimService.releaseCurrentTask();
        }
    }

    /**
     * Выполняет Task: batch-цикл по диапазону с проверкой fencing на каждой итерации.
     */
    public void executeTask(MigrationTask task, MigrationState migration) {
        String migrationId = task.getMigrationId();

        Optional<MigrationTask> fresh = taskRepository.get(migrationId, task.getTaskId());
        if (fresh.isEmpty() || !claimService.validateOwnership(fresh.get())) {
            claimService.releaseCurrentTask();
            return;
        }

        MigrationTask current = fresh.get();
        ReshardingRootProperties.TableConfig tableConfig =
                tableMigrationProperties.getEnabledTable(current.getTableConfigKey());
        if (tableConfig == null) {
            log.error("Table config not found or disabled: {}", current.getTableConfigKey());
            claimService.releaseCurrentTask();
            return;
        }

        // Соединения к shard поднимаются здесь: Task захвачена и migration активна.
        dataSourceLifecycleManager.activateForActiveMigration();
        dataSourceLifecycleManager.beginOperation();

        try {
            current.setStatus(TaskStatus.RUNNING);
            current.setUpdatedAt(LocalDateTime.now());
            if (!claimService.updateOwnedTask(current)) {
                log.warn("Task {} lost ownership before RUNNING transition", current.getTaskId());
                claimService.releaseCurrentTask();
                return;
            }

            boolean idempotentInsert = insertGuardPolicy.requiresIdempotentInsert(current, migration);
            BatchSizing sizing = batchSizeResolver.resolve(current, migration, tableConfig, idempotentInsert);

            if (runBatchLoop(current, tableConfig, sizing)) {
                claimService.completeTask(migrationId, current);
                log.info("Task {} completed range [{} .. {})",
                        current.getTaskId(), current.getRangeFrom(), current.getRangeTo());
            }
        } catch (RuntimeException e) {
            log.error("Task {} failed; releasing for reassignment", current.getTaskId(), e);
            claimService.releaseCurrentTask();
            throw e;
        } finally {
            dataSourceLifecycleManager.endOperation();
        }
    }

    /**
     * Прогоняет batch до исчерпания диапазона.
     *
     * @return {@code true} если диапазон обработан полностью и Task можно завершить
     */
    private boolean runBatchLoop(
            MigrationTask task,
            ReshardingRootProperties.TableConfig tableConfig,
            BatchSizing sizing) {

        TaskCheckpoint cursor = task.getCheckpoint() != null ? task.getCheckpoint() : new TaskCheckpoint();
        long totalRows = 0;

        while (true) {
            BatchResult result = batchProcessor.processBatch(task, tableConfig, sizing, cursor);
            totalRows += result.getRowsRead();

            if (!claimService.validateOwnership(task)) {
                log.warn("Task {} lost ownership after {} rows; stopping", task.getTaskId(), totalRows);
                claimService.releaseCurrentTask();
                return false;
            }

            if (result.getRowsRead() > 0) {
                assertCursorAdvanced(task, cursor, result.getCheckpoint());
            }

            cursor = result.getCheckpoint();
            task.setCheckpoint(cursor);
            task.setUpdatedAt(LocalDateTime.now());
            /*
             * Не используем blind put: Worker мог потерять lease, а Leader уже
             * переназначить Task. Fenced CAS сравнивает modRevision и claim-token;
             * zombie Worker не имеет права публиковать свой cursor.
             */
            if (!claimService.updateOwnedTask(task)) {
                log.warn("Task {} lost fencing CAS after {} rows; stopping",
                        task.getTaskId(), totalRows);
                claimService.releaseCurrentTask();
                return false;
            }

            if (!sameMigrationIsRunning(task.getMigrationId())) {
                log.info("Migration {} is no longer RUNNING; task {} paused at cursor ({}, {})",
                        task.getMigrationId(),
                        task.getTaskId(),
                        cursor.getLastCreatedAt(),
                        cursor.getLastId());
                claimService.releaseForRetry(task);
                return false;
            }

            if (result.rangeExhausted(sizing.getReadBatchSize())) {
                log.debug("Task {} range exhausted after {} rows", task.getTaskId(), totalRows);
                return true;
            }
        }
    }

    private boolean sameMigrationIsRunning(String migrationId) {
        return migrationStateRepository.getCurrent()
                .filter(state -> migrationId.equals(state.getMigrationId()))
                .filter(state -> state.getStatus() == MigrationStatus.RUNNING)
                .isPresent();
    }

    /**
     * Защита от бесконечного цикла: keyset-курсор обязан двигаться вперёд.
     * Если он не сдвинулся, повторное чтение вернёт тот же batch.
     */
    private void assertCursorAdvanced(MigrationTask task, TaskCheckpoint before, TaskCheckpoint after) {
        boolean sameTime = before.getLastCreatedAt() != null
                && before.getLastCreatedAt().isEqual(after.getLastCreatedAt());
        boolean sameId = before.getLastId() != null && before.getLastId().equals(after.getLastId());

        if (sameTime && sameId) {
            throw new BatchProcessingException(
                    "Keyset cursor did not advance for task %s at (%s, %s)"
                            .formatted(task.getTaskId(), after.getLastCreatedAt(), after.getLastId()));
        }
    }
}
