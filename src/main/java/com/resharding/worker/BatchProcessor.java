package com.resharding.worker;

import com.resharding.config.ReshardingRootProperties;
import com.resharding.db.DataSourceRegistry;
import com.resharding.db.SqlIdentifiers;
import com.resharding.db.TargetIndexEnsurer;
import com.resharding.db.TargetRowInserter;
import com.resharding.domain.MigrationTask;
import com.resharding.domain.TaskCheckpoint;
import com.resharding.migration.ShardCalculator;
import com.resharding.migration.ShardKeyResolver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Batch-обработчик данных внутри Task.
 *
 * <p>Читает один batch keyset-пагинацией и переносит строки на target shard.
 * Цикл по всему диапазону Task ведёт {@link WorkerExecutor}: один вызов
 * {@code processRange} обрабатывает ровно один batch и возвращает
 * {@link BatchResult} с позицией курсора.
 *
 * <p>Каждая запись выполняется атомарно через PostgreSQL migration-ledger
 * по глобально уникальному shard key ({@link TargetRowInserter}).
 */
@Slf4j
@Component
public class BatchProcessor {

    private final DataSourceRegistry dataSourceRegistry;
    private final ShardCalculator shardCalculator;
    private final TargetIndexEnsurer targetIndexEnsurer;
    private final ShardKeyResolver shardKeyResolver;
    private final TargetRowInserter targetRowInserter;

    public BatchProcessor(
            DataSourceRegistry dataSourceRegistry,
            ShardCalculator shardCalculator,
            TargetIndexEnsurer targetIndexEnsurer,
            ShardKeyResolver shardKeyResolver,
            TargetRowInserter targetRowInserter) {
        this.dataSourceRegistry = dataSourceRegistry;
        this.shardCalculator = shardCalculator;
        this.targetIndexEnsurer = targetIndexEnsurer;
        this.shardKeyResolver = shardKeyResolver;
        this.targetRowInserter = targetRowInserter;
    }

    /**
     * Обрабатывает один batch диапазона Task.
     *
     * @param task    выполняемая Task
     * @param sizing  размеры batch и режим вставки
     * @param cursor  позиция, с которой продолжать чтение
     */
    public BatchResult processBatch(
            MigrationTask task,
            ReshardingRootProperties.TableConfig tableConfig,
            BatchSizing sizing,
            TaskCheckpoint cursor) {

        int batchSize = sizing.getReadBatchSize();
        boolean continuing = cursor.getLastCreatedAt() != null && cursor.getLastId() != null;

        DataSource sourceDs = dataSourceRegistry.get(task.getSourceDb());
        /*
         * Task ranges строго полуоткрытые: [rangeFrom, rangeTo).
         * Первый запрос использует created_at >= rangeFrom и не подставляет
         * искусственный lastId=0 — иначе строки с нулевым/отрицательным PK
         * на левой границе были бы потеряны. Последующие запросы продолжаются
         * по составному keyset cursor (created_at, id).
         */
        String selectSql = buildSelectSql(task, tableConfig, continuing);

        try (Connection sourceConn = sourceDs.getConnection();
             PreparedStatement ps = sourceConn.prepareStatement(selectSql)) {

            if (continuing) {
                ps.setObject(1, cursor.getLastCreatedAt());
                ps.setLong(2, cursor.getLastId());
                ps.setObject(3, task.getRangeTo());
                ps.setInt(4, batchSize);
            } else {
                ps.setObject(1, task.getRangeFrom());
                ps.setObject(2, task.getRangeTo());
                ps.setInt(3, batchSize);
            }

            List<Map<String, Object>> batch = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    batch.add(readRow(rs));
                }
            }

            if (batch.isEmpty()) {
                TaskCheckpoint end = TaskCheckpoint.builder()
                        .lastCreatedAt(task.getRangeTo())
                        .lastId(cursor.getLastId())
                        .build();
                return new BatchResult(end, 0, 0);
            }

            int inserted = 0;
            for (Map<String, Object> row : batch) {
                if (processRow(task, tableConfig, sizing, row)) {
                    inserted++;
                }
            }

            Map<String, Object> last = batch.getLast();
            TaskCheckpoint next = TaskCheckpoint.builder()
                    .lastCreatedAt((LocalDateTime) last.get(tableConfig.getRangeColumn()))
                    .lastId(((Number) last.get(tableConfig.getIdColumn())).longValue())
                    .build();

            return new BatchResult(next, batch.size(), inserted);

        } catch (Exception e) {
            throw new BatchProcessingException("Failed to process batch for task " + task.getTaskId(), e);
        }
    }

    String buildSelectSql(
            MigrationTask task,
            ReshardingRootProperties.TableConfig tableConfig,
            boolean continuing) {
        String table = SqlIdentifiers.quoteQualified(task.getTable());
        String rangeColumn = SqlIdentifiers.quote(tableConfig.getRangeColumn());
        String idColumn = SqlIdentifiers.quote(tableConfig.getIdColumn());
        return continuing
                ? """
                    SELECT *
                    FROM %s
                    WHERE (%s, %s) > (?, ?)
                      AND %s < ?
                    ORDER BY %s, %s
                    LIMIT ?
                    """.formatted(table, rangeColumn, idColumn, rangeColumn, rangeColumn, idColumn)
                : """
                    SELECT *
                    FROM %s
                    WHERE %s >= ?
                      AND %s < ?
                    ORDER BY %s, %s
                    LIMIT ?
                    """.formatted(table, rangeColumn, rangeColumn, rangeColumn, idColumn);
    }

    /** @return {@code true} если строка вставлена на target */
    private boolean processRow(
            MigrationTask task,
            ReshardingRootProperties.TableConfig tableConfig,
            BatchSizing sizing,
            Map<String, Object> row) {

        Object shardKeyValue = shardKeyResolver.resolveShardKeyValue(tableConfig, row);
        String shardKeyField = shardKeyResolver.resolveShardKeyField(tableConfig, row);
        if (shardKeyValue == null) {
            throw new BatchProcessingException(
                    "Globally unique shard key %s is null for table %s"
                            .formatted(shardKeyField, task.getTable()));
        }
        String targetShard = shardCalculator.resolveTargetShard(
                shardKeyValue, task.getTargetTopology(), tableConfig);

        if (targetShard.equals(task.getSourceDb())) {
            return false;
        }

        long rowId = ((Number) row.get(tableConfig.getIdColumn())).longValue();
        String idColumn = tableConfig.getIdColumn();

        /*
         * Ledger используется и на первом проходе, и на reclaim. Это закрывает
         * окно «INSERT в PostgreSQL успешен, но Worker упал до etcd checkpoint».
         * Дедупликация идёт по глобально уникальному hash key, а не по локальному PK.
         */
        boolean inserted = targetRowInserter.insertWithLedger(
                targetShard,
                task.getMigrationId(),
                task.getTable(),
                shardKeyField,
                shardKeyValue,
                row,
                idColumn);

        if (!inserted) {
            log.debug("Skip insert: source id={} already on {} by shard key {}={}",
                    rowId, targetShard, shardKeyField, shardKeyValue);
            return false;
        }

        LocalDateTime rowDate = (LocalDateTime) row.get(tableConfig.getRangeColumn());
        boolean dateVerifyReady = targetIndexEnsurer.ensureBeforeVerify(
                task.getMigrationId(),
                targetShard,
                task.getTable(),
                tableConfig,
                rowDate,
                task.getCurrentTopology(),
                task.getTargetTopology());

        if (!dateVerifyReady) {
            log.debug("Date verify deferred for row id={} on {} (partition month not yet complete)",
                    rowId, targetShard);
        }
        return true;
    }

    private Map<String, Object> readRow(ResultSet rs) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            row.put(meta.getColumnLabel(i), rs.getObject(i));
        }
        return row;
    }
}
