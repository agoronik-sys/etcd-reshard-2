package com.resharding.leader;

import com.resharding.config.MigrationProperties;
import com.resharding.config.ReshardingRootProperties;
import com.resharding.config.TableMigrationProperties;
import com.resharding.config.TopologyProperties;
import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationTask;
import com.resharding.domain.TableCheckpoint;
import com.resharding.domain.TaskStatus;
import com.resharding.etcd.CheckpointRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Планировщик Task для sliding window.
 *
 * <p>Диапазоны создаются <strong>динамически</strong> по мере продвижения migration;
 * все диапазоны заранее в {@code etcd} не генерируются.
 *
 * <p>Начало следующего диапазона для таблицы:
 * <pre>
 * rangeStart = max(
 *     bulk checkpoint.lastProcessedCreatedAt,
 *     max(rangeTo) уже активных Task этой таблицы,
 *     migration.dateFrom)
 * </pre>
 * Это обеспечивает продвижение окна и отсутствие перекрытия диапазонов
 * между одновременно выполняемыми Task.
 *
 * <p>{@code taskId} детерминирован ({@code task-{table}-{rangeFrom}}): повторное
 * планирование того же диапазона перезапишет тот же ключ вместо создания дубля,
 * в том числе после смены Leader.
 */
@Slf4j
@Component
public class TaskPlanner {

    private static final DateTimeFormatter TASK_ID_TIME = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");

    private final MigrationProperties migrationProperties;
    private final TableMigrationProperties tableMigrationProperties;
    private final TopologyProperties topologyProperties;
    private final CheckpointRepository checkpointRepository;

    public TaskPlanner(
            MigrationProperties migrationProperties,
            TableMigrationProperties tableMigrationProperties,
            TopologyProperties topologyProperties,
            CheckpointRepository checkpointRepository) {
        this.migrationProperties = migrationProperties;
        this.tableMigrationProperties = tableMigrationProperties;
        this.topologyProperties = topologyProperties;
        this.checkpointRepository = checkpointRepository;
    }

    /**
     * Планирует следующие Task в пределах доступных слотов sliding window.
     *
     * <p>Task создаются для каждой пары (таблица × source shard): данные
     * переносятся со всех shard current topology, а не только с первого.
     *
     * @param migration      текущая migration
     * @param slotsAvailable сколько Task можно создать в этом цикле
     * @param activeTasks    уже существующие активные Task
     * @return список новых Task для сохранения в {@code etcd}
     */
    public List<MigrationTask> planNextTasks(
            MigrationState migration,
            int slotsAvailable,
            List<MigrationTask> activeTasks) {

        if (slotsAvailable <= 0) {
            return List.of();
        }

        List<MigrationTask> planned = new ArrayList<>();

        for (var entry : tableMigrationProperties.getEnabledTables().entrySet()) {
            for (String sourceDb : sourceShards(migration)) {
                if (planned.size() >= slotsAvailable) {
                    return planned;
                }

                planForTableShard(migration, entry.getKey(), entry.getValue(), sourceDb, activeTasks, planned)
                        .ifPresent(planned::add);
            }
        }

        return planned;
    }

    private Optional<MigrationTask> planForTableShard(
            MigrationState migration,
            String tableConfigKey,
            ReshardingRootProperties.TableConfig tableConfig,
            String sourceDb,
            List<MigrationTask> activeTasks,
            List<MigrationTask> plannedInThisCycle) {

        String table = tableConfig.resolveTableName(tableConfigKey);

        LocalDateTime rangeStart = nextRangeStart(migration, table, sourceDb, activeTasks, plannedInThisCycle);
        if (!rangeStart.isBefore(migration.getDateTo())) {
            return Optional.empty();
        }

        LocalDateTime rangeEnd = rangeStart.plusMinutes(migrationProperties.getRangeDurationMinutes());
        if (rangeEnd.isAfter(migration.getDateTo())) {
            rangeEnd = migration.getDateTo();
        }

        MigrationTask task = MigrationTask.builder()
                .taskId(buildTaskId(table, sourceDb, rangeStart))
                .migrationId(migration.getMigrationId())
                .tableConfigKey(tableConfigKey)
                .table(table)
                .sourceDb(sourceDb)
                .targetTopology(migration.getTargetTopology())
                .currentTopology(migration.getCurrentTopology())
                .rangeType(tableConfig.getRangeStrategy())
                .rangeFrom(rangeStart)
                .rangeTo(rangeEnd)
                .status(TaskStatus.CREATED)
                .generation(0)
                .sequenceNumber(nextSequenceNumber(activeTasks, plannedInThisCycle))
                .startedAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        log.debug("Planned task {} for {}.{} range [{} .. {})",
                task.getTaskId(), sourceDb, table, rangeStart, rangeEnd);
        return Optional.of(task);
    }

    /**
     * Начало следующего непокрытого диапазона для пары (таблица, source shard).
     */
    LocalDateTime nextRangeStart(
            MigrationState migration,
            String table,
            String sourceDb,
            List<MigrationTask> activeTasks,
            List<MigrationTask> plannedInThisCycle) {

        LocalDateTime position = checkpointRepository
                .getBulkCheckpoint(migration.getMigrationId(), table, sourceDb)
                .map(TableCheckpoint::getLastProcessedCreatedAt)
                .filter(value -> value != null && value.isAfter(migration.getDateFrom()))
                .orElse(migration.getDateFrom());

        LocalDateTime inFlight = maxRangeTo(activeTasks, table, sourceDb);
        if (inFlight != null && inFlight.isAfter(position)) {
            position = inFlight;
        }

        LocalDateTime justPlanned = maxRangeTo(plannedInThisCycle, table, sourceDb);
        if (justPlanned != null && justPlanned.isAfter(position)) {
            position = justPlanned;
        }

        return position;
    }

    private LocalDateTime maxRangeTo(List<MigrationTask> tasks, String table, String sourceDb) {
        return tasks.stream()
                .filter(t -> table.equals(t.getTable()) && sourceDb.equals(t.getSourceDb()))
                .map(MigrationTask::getRangeTo)
                .filter(java.util.Objects::nonNull)
                .max(LocalDateTime::compareTo)
                .orElse(null);
    }

    private int nextSequenceNumber(List<MigrationTask> activeTasks, List<MigrationTask> plannedInThisCycle) {
        int fromActive = activeTasks.stream().mapToInt(MigrationTask::getSequenceNumber).max().orElse(0);
        int fromPlanned = plannedInThisCycle.stream().mapToInt(MigrationTask::getSequenceNumber).max().orElse(0);
        return Math.max(fromActive, fromPlanned) + 1;
    }

    private List<String> sourceShards(MigrationState migration) {
        ReshardingRootProperties.TopologyDefinition topology =
                topologyProperties.getTopologies().get(migration.getCurrentTopology());
        if (topology == null || topology.getShards() == null || topology.getShards().isEmpty()) {
            throw new IllegalStateException("Unknown current topology: " + migration.getCurrentTopology());
        }
        return topology.getShards();
    }

    private String buildTaskId(String table, String sourceDb, LocalDateTime rangeFrom) {
        return "task-%s-%s-%s".formatted(table, sourceDb, rangeFrom.format(TASK_ID_TIME));
    }
}
