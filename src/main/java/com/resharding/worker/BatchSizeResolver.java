package com.resharding.worker;

import com.resharding.config.MigrationProperties;
import com.resharding.config.ReshardingRootProperties;
import com.resharding.config.TopologyProperties;
import com.resharding.domain.InsertMode;
import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationTask;
import com.resharding.migration.NewShardResolver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

/**
 * Вычисляет effective размер batch с учётом режима вставки и типа DB shard.
 *
 * <ul>
 *   <li>идемпотентный INSERT (reclaim / {@code checkBeforeInsert}) — маленькие пачки
 *       ({@code migration.idempotent-insert-batch-size}, по умолчанию 500);</li>
 *   <li>COPY на новые shard — большие пачки ({@code migration.copy-batch-size}, 50 000);</li>
 *   <li>чтение/запись с активного source shard (current topology, рабочая БД с индексами) —
 *       batch уменьшается в {@code migration.active-source-batch-divisor} раз (по умолчанию 10),
 *       чтобы снизить нагрузку и autovacuum.</li>
 * </ul>
 */
@Slf4j
@Component
public class BatchSizeResolver {

    private final MigrationProperties migrationProperties;
    private final TopologyProperties topologyProperties;
    private final NewShardResolver newShardResolver;

    public BatchSizeResolver(
            MigrationProperties migrationProperties,
            TopologyProperties topologyProperties,
            NewShardResolver newShardResolver) {
        this.migrationProperties = migrationProperties;
        this.topologyProperties = topologyProperties;
        this.newShardResolver = newShardResolver;
    }

    /**
     * Размер batch для READ из source в рамках Task.
     */
    public BatchSizing resolve(
            MigrationTask task,
            MigrationState migration,
            ReshardingRootProperties.TableConfig tableConfig,
            boolean idempotentInsert) {

        boolean activeSource = isActiveSourceShard(task.getSourceDb(), migration.getCurrentTopology());

        if (idempotentInsert) {
            int size = migrationProperties.getIdempotentInsertBatchSize();
            log.debug("Task {}: idempotent insert batch size {} (active source={})",
                    task.getTaskId(), size, activeSource);
            return new BatchSizing(size, size, InsertMode.IDEMPOTENT_INSERT, activeSource);
        }

        int copyBatch = resolveCopyBatchSize(tableConfig);
        int readBatch = copyBatch;
        if (activeSource) {
            readBatch = reduceForActiveSource(copyBatch);
        }

        log.debug("Task {}: copy read batch {}, write batch {} (active source={})",
                task.getTaskId(), readBatch, copyBatch, activeSource);

        return new BatchSizing(readBatch, copyBatch, InsertMode.COPY, activeSource);
    }

    /**
     * Размер batch записи и режим для конкретного target shard.
     */
    public BatchSizing resolveForTarget(
            MigrationTask task,
            MigrationState migration,
            ReshardingRootProperties.TableConfig tableConfig,
            String targetDbId,
            boolean idempotentInsert) {

        BatchSizing base = resolve(task, migration, tableConfig, idempotentInsert);
        if (base.getInsertMode() == InsertMode.IDEMPOTENT_INSERT) {
            return base;
        }

        boolean targetIsNew = newShardResolver.isNewShard(
                targetDbId, task.getCurrentTopology(), task.getTargetTopology());

        if (targetIsNew) {
            return new BatchSizing(base.getReadBatchSize(), base.getWriteBatchSize(), InsertMode.COPY, base.isActiveSource());
        }

        int reducedWrite = reduceForActiveSource(base.getWriteBatchSize());
        return new BatchSizing(base.getReadBatchSize(), reducedWrite, InsertMode.ROW_INSERT, base.isActiveSource());
    }

    /** Shard входит в current topology — рабочая БД, которую ещё используют как source. */
    public boolean isActiveSourceShard(String dbId, String currentTopology) {
        return shardsOf(currentTopology).contains(dbId);
    }

    private int resolveCopyBatchSize(ReshardingRootProperties.TableConfig tableConfig) {
        if (tableConfig.getBatchSize() > 0) {
            return tableConfig.getBatchSize();
        }
        return migrationProperties.getCopyBatchSize();
    }

    private int reduceForActiveSource(int batch) {
        int divisor = migrationProperties.getActiveSourceBatchDivisor();
        return Math.max(1, batch / divisor);
    }

    private Set<String> shardsOf(String topologyVersion) {
        var topology = topologyProperties.getTopologies().get(topologyVersion);
        if (topology == null || topology.getShards() == null) {
            return Set.of();
        }
        return new HashSet<>(topology.getShards());
    }
}
