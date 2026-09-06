package com.resharding.leader;

import com.resharding.cluster.LeaderElectionService;
import com.resharding.cluster.MemberRegistryService;
import com.resharding.config.MigrationProperties;
import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationStatus;
import com.resharding.domain.MigrationTask;
import com.resharding.domain.TaskStatus;
import com.resharding.etcd.MigrationStateRepository;
import com.resharding.etcd.ActiveMigrationLockRepository;
import com.resharding.etcd.TaskRepository;
import com.resharding.etcd.CheckpointRepository;
import com.resharding.leader.slot.DatabaseSlotManager;
import com.resharding.worker.WorkerTaskClaimService;
import com.resharding.db.DataSourceLifecycleManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Периодический цикл Leader для управления активной migration.
 *
 * <p>Выполняется только на Pod с {@link LeaderElectionService#isLeader()}.
 * Каждый tick реализует алгоритм из ТЗ:
 * <ol>
 *   <li>проверка текущей migration;</li>
 *   <li>освобождение stale Task (истёк lock упавшего Worker);</li>
 *   <li>последовательный сдвиг checkpoint;</li>
 *   <li>удаление завершённых Task из {@code etcd};</li>
 *   <li>создание новых Task в пределах sliding window;</li>
 *   <li>incremental cycle;</li>
 *   <li>проверка условия завершения migration.</li>
 * </ol>
 *
 * <p>Число одновременных Task: {@code min(availableWorkers, maxConcurrentTasks, databaseLimits)}.
 */
@Slf4j
@Service
public class LeaderScheduler {

    private final LeaderElectionService leaderElectionService;
    private final MemberRegistryService memberRegistryService;
    private final MigrationStateRepository migrationStateRepository;
    private final TaskRepository taskRepository;
    private final TaskPlanner taskPlanner;
    private final DatabaseSlotManager databaseSlotManager;
    private final WorkerTaskClaimService workerTaskClaimService;
    private final MigrationProperties migrationProperties;
    private final IncrementalCycleRunner incrementalCycleRunner;
    private final CheckpointAdvancer checkpointAdvancer;
    private final BulkCompletionEvaluator bulkCompletionEvaluator;
    private final PartitionIndexCoordinator partitionIndexCoordinator;
    private final ActiveMigrationLockRepository activeMigrationLockRepository;
    private final CheckpointRepository checkpointRepository;
    private final DataSourceLifecycleManager dataSourceLifecycleManager;

    public LeaderScheduler(
            LeaderElectionService leaderElectionService,
            MemberRegistryService memberRegistryService,
            MigrationStateRepository migrationStateRepository,
            TaskRepository taskRepository,
            TaskPlanner taskPlanner,
            DatabaseSlotManager databaseSlotManager,
            WorkerTaskClaimService workerTaskClaimService,
            MigrationProperties migrationProperties,
            IncrementalCycleRunner incrementalCycleRunner,
            CheckpointAdvancer checkpointAdvancer,
            BulkCompletionEvaluator bulkCompletionEvaluator,
            PartitionIndexCoordinator partitionIndexCoordinator,
            ActiveMigrationLockRepository activeMigrationLockRepository,
            CheckpointRepository checkpointRepository,
            DataSourceLifecycleManager dataSourceLifecycleManager) {
        this.leaderElectionService = leaderElectionService;
        this.memberRegistryService = memberRegistryService;
        this.migrationStateRepository = migrationStateRepository;
        this.taskRepository = taskRepository;
        this.taskPlanner = taskPlanner;
        this.databaseSlotManager = databaseSlotManager;
        this.workerTaskClaimService = workerTaskClaimService;
        this.migrationProperties = migrationProperties;
        this.incrementalCycleRunner = incrementalCycleRunner;
        this.checkpointAdvancer = checkpointAdvancer;
        this.bulkCompletionEvaluator = bulkCompletionEvaluator;
        this.partitionIndexCoordinator = partitionIndexCoordinator;
        this.activeMigrationLockRepository = activeMigrationLockRepository;
        this.checkpointRepository = checkpointRepository;
        this.dataSourceLifecycleManager = dataSourceLifecycleManager;
    }

    /**
     * Основной tick планировщика. No-op если Pod не Leader или migration не RUNNING.
     */
    @Scheduled(
            fixedDelayString = "${migration.scheduler-interval-ms:2000}",
            scheduler = "leaderTaskScheduler")
    public void tick() {
        if (!leaderElectionService.isLeader()) {
            return;
        }

        Optional<MigrationState> migrationOpt = migrationStateRepository.getCurrent();
        if (migrationOpt.isEmpty() || migrationOpt.get().getStatus() != MigrationStatus.RUNNING) {
            return;
        }

        MigrationState migration = migrationOpt.get();
        dataSourceLifecycleManager.activateForActiveMigration();
        dataSourceLifecycleManager.beginOperation();
        try {
            runSchedulerCycle(migration);
        } catch (Exception e) {
            log.error("Leader scheduler cycle failed for migration {}", migration.getMigrationId(), e);
        } finally {
            dataSourceLifecycleManager.endOperation();
        }
    }

    private void runSchedulerCycle(MigrationState migration) {
        String migrationId = migration.getMigrationId();
        ensureActiveMigrationLock(migrationId);
        List<MigrationTask> allTasks = taskRepository.listAll(migrationId);
        databaseSlotManager.reconcileSlots(migrationId, allTasks);

        releaseStaleTasks(migrationId, allTasks);
        checkpointAdvancer.advance(migration, allTasks);
        cleanupCompletedTasks(migrationId, allTasks);

        List<MigrationTask> activeTasks = allTasks.stream()
                .filter(task -> task.getStatus() != TaskStatus.COMPLETED)
                .toList();

        int availableWorkers = memberRegistryService.availableWorkers(leaderElectionService.instanceId());
        int maxTasks = Math.min(availableWorkers, migrationProperties.getMaxConcurrentTasks());
        int slotsAvailable = Math.max(0, maxTasks - countRunningTasks(activeTasks));

        if (slotsAvailable > 0) {
            List<MigrationTask> planned = taskPlanner.planNextTasks(migration, slotsAvailable, activeTasks);
            for (MigrationTask task : planned) {
                if (databaseSlotManager.tryAcquireSlots(task)) {
                    taskRepository.save(migrationId, task);
                    log.info("Created task {} for {}.{} range [{} .. {})",
                            task.getTaskId(), task.getSourceDb(), task.getTable(),
                            task.getRangeFrom(), task.getRangeTo());
                }
            }
        }

        incrementalCycleRunner.runIfDue(migration);
        tryCompleteMigration(migration);
    }

    /**
     * Возвращает в пул Task, чей lock истёк: Worker Pod упал, не завершив диапазон.
     *
     * <p>{@code generation} увеличивается — следующий Worker увидит
     * {@link MigrationTask#isReclaimed()} и включит идемпотентную вставку.
     */
    private void releaseStaleTasks(String migrationId, List<MigrationTask> tasks) {
        for (MigrationTask task : tasks) {
            boolean owned = task.getStatus() == TaskStatus.RUNNING || task.getStatus() == TaskStatus.ASSIGNED;
            if (!owned) {
                continue;
            }

            if (workerTaskClaimService.isTaskLockExpired(migrationId, task.getTaskId())) {
                taskRepository.getVersioned(migrationId, task.getTaskId()).ifPresent(current -> {
                    MigrationTask persisted = current.task();
                    boolean stillOwned = persisted.getStatus() == TaskStatus.RUNNING
                            || persisted.getStatus() == TaskStatus.ASSIGNED;
                    if (!stillOwned) {
                        return;
                    }
                    persisted.setStatus(TaskStatus.AVAILABLE);
                    persisted.setWorkerId(null);
                    persisted.setClaimToken(null);
                    persisted.setUpdatedAt(LocalDateTime.now());

                    /*
                     * CAS проверяет одновременно revision Task и отсутствие lock.
                     * Если Worker успел продлить lease/завершить Task, Leader не
                     * перезапишет более новое состояние stale-снимком.
                     */
                    boolean released = taskRepository.releaseStale(
                            migrationId,
                            persisted,
                            current.revision(),
                            workerTaskClaimService.taskLockKey(migrationId, persisted.getTaskId()));
                    if (released) {
                        task.setStatus(TaskStatus.AVAILABLE);
                        task.setWorkerId(null);
                        task.setClaimToken(null);
                        log.info("Released stale task {} generation {}",
                                persisted.getTaskId(), persisted.getGeneration());
                    }
                });
            }
        }
    }

    private void cleanupCompletedTasks(String migrationId, List<MigrationTask> tasks) {
        tasks.stream()
                .filter(task -> task.getStatus() == TaskStatus.COMPLETED)
                .sorted(Comparator.comparingInt(MigrationTask::getSequenceNumber))
                .forEach(task -> {
                    /*
                     * COMPLETED ещё не означает, что диапазон отражён глобальным
                     * checkpoint: продвижение могло остановиться на gap или CAS
                     * conflict. Удаление разрешено только после persisted
                     * checkpoint >= rangeTo, иначе Leader потеряет доказательство
                     * завершения диапазона и запланирует его повторно.
                     */
                    boolean checkpointCoversTask = checkpointRepository
                            .getBulkCheckpoint(migrationId, task.getTable(), task.getSourceDb())
                            .map(cp -> cp.getLastProcessedCreatedAt() != null
                                    && !cp.getLastProcessedCreatedAt().isBefore(task.getRangeTo()))
                            .orElse(false);
                    if (!checkpointCoversTask) {
                        return;
                    }

                    taskRepository.getVersioned(migrationId, task.getTaskId()).ifPresent(current -> {
                        if (current.task().getStatus() != TaskStatus.COMPLETED) {
                            return;
                        }
                        if (taskRepository.deleteIfRevision(
                                migrationId, task.getTaskId(), current.revision())) {
                            databaseSlotManager.releaseSlots(task);
                            log.debug("Removed checkpointed task {} from etcd", task.getTaskId());
                        }
                    });
                });
    }

    private int countRunningTasks(List<MigrationTask> tasks) {
        return (int) tasks.stream()
                .filter(t -> t.getStatus() == TaskStatus.RUNNING || t.getStatus() == TaskStatus.ASSIGNED)
                .count();
    }

    /**
     * Завершает migration когда bulk полностью перенесён и incremental догнан.
     *
     * <p>Проверяется по checkpoint каждой пары (таблица × source shard),
     * а не по пустому списку Task.
     */
    private void tryCompleteMigration(MigrationState migration) {
        List<MigrationTask> remaining = taskRepository.listActive(migration.getMigrationId());
        if (!remaining.isEmpty()) {
            return;
        }

        if (!bulkCompletionEvaluator.isBulkComplete(migration)) {
            return;
        }

        if (!incrementalCycleRunner.isCaughtUp(migration)) {
            return;
        }

        partitionIndexCoordinator.onBulkCompleted(migration);

        migration.setStatus(MigrationStatus.COMPLETED);
        migration.setUpdatedAt(LocalDateTime.now());
        migrationStateRepository.save(migration);
        if (!activeMigrationLockRepository.release(migration.getMigrationId())) {
            log.warn("Migration {} completed, but active lock changed; not deleting foreign lock",
                    migration.getMigrationId());
        }
        log.info("Migration {} completed", migration.getMigrationId());
    }

    /**
     * Active lock разделяет lease текущего Leader. После failover старый lease
     * исчезает вместе с lock, а новый Leader восстанавливает lock для уже RUNNING
     * migration, не создавая новую migration и не теряя checkpoint.
     */
    private void ensureActiveMigrationLock(String migrationId) {
        var existing = activeMigrationLockRepository.get();
        if (existing.isPresent()) {
            if (!migrationId.equals(existing.get().getMigrationId())) {
                throw new IllegalStateException(
                        "Active lock belongs to another migration " + existing.get().getMigrationId());
            }
            return;
        }
        boolean acquired = activeMigrationLockRepository.tryAcquire(
                migrationId,
                leaderElectionService.instanceId(),
                leaderElectionService.leaderLeaseId());
        if (!acquired) {
            throw new IllegalStateException("Failed to restore active migration lock for " + migrationId);
        }
    }
}
