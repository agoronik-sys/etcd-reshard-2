package com.resharding.leader;

import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationTask;
import com.resharding.domain.TableCheckpoint;
import com.resharding.domain.TaskStatus;
import com.resharding.etcd.CheckpointRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Последовательный сдвиг глобального checkpoint.
 *
 * <p>Инвариант: checkpoint указывает на конец последнего <em>непрерывно</em>
 * завершённого диапазона. Непрерывность проверяется по времени, а не по
 * {@code sequenceNumber}: checkpoint сдвигается на {@code rangeTo} завершённой Task
 * только если её {@code rangeFrom} совпадает с текущей позицией checkpoint.
 *
 * <p>Пример: Task [10:00–11:00) выполняется, Task [11:00–12:00) завершена.
 * Checkpoint остаётся на 10:00 — «дыра» не перескакивается.
 *
 * <p>Прогресс отслеживается отдельно для каждой пары (таблица, source shard).
 *
 * @see TableCheckpoint
 */
@Slf4j
@Component
public class CheckpointAdvancer {

    private final CheckpointRepository checkpointRepository;
    private final PartitionIndexCoordinator partitionIndexCoordinator;

    public CheckpointAdvancer(
            CheckpointRepository checkpointRepository,
            PartitionIndexCoordinator partitionIndexCoordinator) {
        this.checkpointRepository = checkpointRepository;
        this.partitionIndexCoordinator = partitionIndexCoordinator;
    }

    /**
     * Продвигает checkpoint по всем непрерывно завершённым Task.
     *
     * @param migration текущая migration
     * @param allTasks  все Task из {@code etcd}, включая {@link TaskStatus#COMPLETED}
     */
    public void advance(MigrationState migration, List<MigrationTask> allTasks) {
        Map<String, List<MigrationTask>> completedByScope = new LinkedHashMap<>();

        for (MigrationTask task : allTasks) {
            if (task.getStatus() == TaskStatus.COMPLETED && task.getRangeFrom() != null) {
                completedByScope
                        .computeIfAbsent(task.getTable() + "\u0000" + task.getSourceDb(), key -> new java.util.ArrayList<>())
                        .add(task);
            }
        }

        completedByScope.values().forEach(tasks -> advanceScope(migration, tasks));
    }

    private void advanceScope(MigrationState migration, List<MigrationTask> completedTasks) {
        List<MigrationTask> ordered = completedTasks.stream()
                .sorted(Comparator.comparing(MigrationTask::getRangeFrom))
                .toList();

        MigrationTask first = ordered.getFirst();
        String table = first.getTable();
        String sourceDb = first.getSourceDb();

        Optional<TableCheckpoint> current =
                checkpointRepository.getBulkCheckpoint(migration.getMigrationId(), table, sourceDb);
        LocalDateTime position = current
                .map(TableCheckpoint::getLastProcessedCreatedAt)
                .orElse(migration.getDateFrom());

        for (MigrationTask task : ordered) {
            if (!task.getRangeFrom().isEqual(position)) {
                log.debug("Checkpoint for {}.{} stays at {}: task {} starts at {} (gap)",
                        sourceDb, table, position, task.getTaskId(), task.getRangeFrom());
                break;
            }

            boolean written = writeCheckpoint(
                    migration, task, current.map(TableCheckpoint::getRevision).orElse(null));
            if (!written) {
                break;
            }

            partitionIndexCoordinator.onCheckpointAdvanced(task, position, task.getRangeTo());

            position = task.getRangeTo();
            current = checkpointRepository.getBulkCheckpoint(migration.getMigrationId(), table, sourceDb);
        }
    }

    private boolean writeCheckpoint(MigrationState migration, MigrationTask task, Long expectedRevision) {
        TableCheckpoint next = TableCheckpoint.builder()
                .table(task.getTable())
                .sourceDb(task.getSourceDb())
                .lastProcessedCreatedAt(task.getRangeTo())
                .lastProcessedId(task.getCheckpoint() != null ? task.getCheckpoint().getLastId() : null)
                .updatedAt(LocalDateTime.now())
                .build();

        boolean written = expectedRevision == null
                ? checkpointRepository.initBulkCheckpoint(migration.getMigrationId(), next)
                : checkpointRepository.updateBulkCheckpoint(migration.getMigrationId(), next, expectedRevision);

        if (!written) {
            log.warn("Checkpoint CAS lost for {}.{} (task {}); retry on next cycle",
                    task.getSourceDb(), task.getTable(), task.getTaskId());
            return false;
        }

        log.info("Checkpoint advanced for {}.{} to {}", task.getSourceDb(), task.getTable(), task.getRangeTo());
        return true;
    }
}
