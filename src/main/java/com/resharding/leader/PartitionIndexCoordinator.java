package com.resharding.leader;

import com.resharding.config.ReshardingRootProperties;
import com.resharding.config.TableMigrationProperties;
import com.resharding.config.TopologyProperties;
import com.resharding.db.PartitionCompletionDetector;
import com.resharding.db.TargetIndexEnsurer;
import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationTask;
import com.resharding.domain.TableCheckpoint;
import com.resharding.etcd.CheckpointRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.List;

/**
 * Координирует создание verify-индексов на партициях после полного залития месяца.
 *
 * <p>Вызывается Leader'ом при продвижении checkpoint. Только для партиционированных
 * таблиц и только на новых DB shard.
 *
 * <p>Месяц считается залитым, когда его границу прошли checkpoint'ы <strong>всех</strong>
 * source shard таблицы: пока хотя бы один shard не досчитал месяц, партиция на target
 * ещё пополняется и индексировать её рано.
 */
@Slf4j
@Component
public class PartitionIndexCoordinator {

    private final TableMigrationProperties tableMigrationProperties;
    private final TopologyProperties topologyProperties;
    private final CheckpointRepository checkpointRepository;
    private final PartitionCompletionDetector partitionCompletionDetector;
    private final TargetIndexEnsurer targetIndexEnsurer;

    public PartitionIndexCoordinator(
            TableMigrationProperties tableMigrationProperties,
            TopologyProperties topologyProperties,
            CheckpointRepository checkpointRepository,
            PartitionCompletionDetector partitionCompletionDetector,
            TargetIndexEnsurer targetIndexEnsurer) {
        this.tableMigrationProperties = tableMigrationProperties;
        this.topologyProperties = topologyProperties;
        this.checkpointRepository = checkpointRepository;
        this.partitionCompletionDetector = partitionCompletionDetector;
        this.targetIndexEnsurer = targetIndexEnsurer;
    }

    /**
     * Проверяет, завершён ли календарный месяц для таблицы Task, и создаёт индексы на партиции.
     *
     * @param task               завершённая Task
     * @param previousCheckpoint {@code lastProcessedCreatedAt} до обновления
     * @param newCheckpoint      новое значение после завершения Task
     */
    public void onCheckpointAdvanced(
            MigrationTask task,
            LocalDateTime previousCheckpoint,
            LocalDateTime newCheckpoint) {

        ReshardingRootProperties.TableConfig tableConfig =
                tableMigrationProperties.getEnabledTable(task.getTableConfigKey());
        if (tableConfig == null || !tableConfig.isPartitioned()) {
            return;
        }

        for (YearMonth month :
                partitionCompletionDetector.detectCompletedMonths(previousCheckpoint, newCheckpoint)) {
            if (!allSourceShardsPassed(task, month)) {
                log.debug("Month {} for table {} not complete on all source shards yet",
                        month, task.getTable());
                continue;
            }

            log.info("Partition month {} completed for table {} in migration {}",
                    month, task.getTable(), task.getMigrationId());

            targetIndexEnsurer.ensurePartitionIndexesForCompletedMonth(
                    task.getMigrationId(),
                    task.getCurrentTopology(),
                    task.getTargetTopology(),
                    task,
                    tableConfig,
                    month);
        }
    }

    /**
     * Создаёт индексы на партициях последнего (возможно неполного) месяца при завершении bulk migration.
     *
     * <p>{@link #onCheckpointAdvanced} срабатывает только на переходе через границу месяца,
     * поэтому месяц, в котором находится {@code dateTo}, иначе остался бы без индексов.
     */
    public void onBulkCompleted(MigrationState migration) {
        for (var entry : tableMigrationProperties.getEnabledTables().entrySet()) {
            ReshardingRootProperties.TableConfig tableConfig = entry.getValue();
            if (!tableConfig.isPartitioned()) {
                continue;
            }

            String table = tableConfig.resolveTableName(entry.getKey());
            // dateTo — exclusive. Если это ровно 1-е число месяца, последняя
            // заполненная партиция относится к предыдущему месяцу.
            YearMonth finalMonth = YearMonth.from(migration.getDateTo().minusNanos(1));

            MigrationTask pseudoTask = MigrationTask.builder()
                    .migrationId(migration.getMigrationId())
                    .tableConfigKey(entry.getKey())
                    .table(table)
                    .currentTopology(migration.getCurrentTopology())
                    .targetTopology(migration.getTargetTopology())
                    .build();

            log.info("Bulk complete: ensuring partition indexes for final month {} of table {}", finalMonth, table);
            targetIndexEnsurer.ensurePartitionIndexesForCompletedMonth(
                    migration.getMigrationId(),
                    migration.getCurrentTopology(),
                    migration.getTargetTopology(),
                    pseudoTask,
                    tableConfig,
                    finalMonth);
        }
    }

    /** {@code true}, если checkpoint каждого source shard таблицы вышел за пределы месяца. */
    private boolean allSourceShardsPassed(MigrationTask task, YearMonth month) {
        LocalDateTime monthEnd = month.plusMonths(1).atDay(1).atStartOfDay();

        for (String sourceDb : sourceShards(task.getCurrentTopology())) {
            LocalDateTime position = checkpointRepository
                    .getBulkCheckpoint(task.getMigrationId(), task.getTable(), sourceDb)
                    .map(TableCheckpoint::getLastProcessedCreatedAt)
                    .orElse(null);

            if (position == null || position.isBefore(monthEnd)) {
                return false;
            }
        }
        return true;
    }

    private List<String> sourceShards(String currentTopology) {
        ReshardingRootProperties.TopologyDefinition topology =
                topologyProperties.getTopologies().get(currentTopology);
        if (topology == null || topology.getShards() == null) {
            return List.of();
        }
        return topology.getShards();
    }
}
