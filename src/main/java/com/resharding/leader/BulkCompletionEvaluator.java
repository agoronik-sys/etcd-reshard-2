package com.resharding.leader;

import com.resharding.config.ReshardingRootProperties;
import com.resharding.config.TableMigrationProperties;
import com.resharding.config.TopologyProperties;
import com.resharding.domain.MigrationState;
import com.resharding.domain.TableCheckpoint;
import com.resharding.etcd.CheckpointRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Проверка фактического завершения bulk migration.
 *
 * <p>Bulk считается завершённым только когда checkpoint <strong>каждой</strong>
 * пары (включённая таблица × source shard) достиг {@code dateTo}.
 * Пустой список активных Task сам по себе ничего не доказывает: Task могли
 * не создаться из-за отсутствия Worker или исчерпанных лимитов DB.
 */
@Slf4j
@Component
public class BulkCompletionEvaluator {

    private final TableMigrationProperties tableMigrationProperties;
    private final TopologyProperties topologyProperties;
    private final CheckpointRepository checkpointRepository;

    public BulkCompletionEvaluator(
            TableMigrationProperties tableMigrationProperties,
            TopologyProperties topologyProperties,
            CheckpointRepository checkpointRepository) {
        this.tableMigrationProperties = tableMigrationProperties;
        this.topologyProperties = topologyProperties;
        this.checkpointRepository = checkpointRepository;
    }

    /** {@code true} если все диапазоны {@code dateFrom..dateTo} перенесены. */
    public boolean isBulkComplete(MigrationState migration) {
        if (migration.getDateTo() == null) {
            return false;
        }

        List<String> sourceShards = sourceShards(migration.getCurrentTopology());
        if (sourceShards.isEmpty()) {
            return false;
        }

        for (var entry : tableMigrationProperties.getEnabledTables().entrySet()) {
            String table = entry.getValue().resolveTableName(entry.getKey());

            for (String sourceDb : sourceShards) {
                LocalDateTime position = checkpointRepository
                        .getBulkCheckpoint(migration.getMigrationId(), table, sourceDb)
                        .map(TableCheckpoint::getLastProcessedCreatedAt)
                        .orElse(null);

                if (position == null || position.isBefore(migration.getDateTo())) {
                    log.debug("Bulk not complete: {}.{} checkpoint at {} (target {})",
                            sourceDb, table, position, migration.getDateTo());
                    return false;
                }
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
