package com.resharding.migration;

import com.resharding.config.ReshardingRootProperties;
import com.resharding.config.TopologyProperties;
import com.resharding.domain.ShardAlgorithm;
import com.resharding.migration.ketama.KetamaRingFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Детерминированный расчёт target shard по shard key и topology.
 *
 * <p>При выборке batch записей из source для каждой строки определяется target DB:
 * <ol>
 *   <li>{@link com.resharding.migration.ShardKeyResolver} выбирает поле shard key;</li>
 *   <li>{@link ShardAlgorithm#KETAMA} — consistent hash как nginx upstream ketama;</li>
 *   <li>если target shard ≠ source — запись переносится.</li>
 * </ol>
 */
@Component
public class ShardCalculator {

    private final TopologyProperties topologyProperties;
    private final KetamaRingFactory ketamaRingFactory;

    public ShardCalculator(TopologyProperties topologyProperties, KetamaRingFactory ketamaRingFactory) {
        this.topologyProperties = topologyProperties;
        this.ketamaRingFactory = ketamaRingFactory;
    }

    /**
     * Target shard для записи с учётом алгоритма из конфигурации таблицы.
     */
    public String resolveTargetShard(
            Object shardKeyValue,
            String topologyVersion,
            ReshardingRootProperties.TableConfig tableConfig) {

        ShardAlgorithm algorithm = ShardAlgorithm.from(tableConfig.getShardAlgorithm());
        return switch (algorithm) {
            case KETAMA -> ketamaRingFactory.getRing(topologyVersion).locate(shardKeyValue);
            case HASH_MOD -> hashModShard(shardKeyValue, topologyVersion);
        };
    }

    /** @deprecated используйте {@link #resolveTargetShard(Object, String, ReshardingRootProperties.TableConfig)} */
    public String resolveTargetShard(Object shardKeyValue, String topologyVersion) {
        return hashModShard(shardKeyValue, topologyVersion);
    }

    public boolean needsMigration(
            String sourceDb,
            Object shardKeyValue,
            String targetTopologyVersion,
            ReshardingRootProperties.TableConfig tableConfig) {

        String targetShard = resolveTargetShard(shardKeyValue, targetTopologyVersion, tableConfig);
        return !sourceDb.equals(targetShard);
    }

    public boolean needsMigration(String sourceDb, Object shardKeyValue, String targetTopologyVersion) {
        String targetShard = resolveTargetShard(shardKeyValue, targetTopologyVersion);
        return !sourceDb.equals(targetShard);
    }

    private String hashModShard(Object shardKeyValue, String topologyVersion) {
        List<String> shards = shardsOf(topologyVersion);
        int index = hashMod(shardKeyValue, shards.size());
        return shards.get(index);
    }

    private List<String> shardsOf(String topologyVersion) {
        ReshardingRootProperties.TopologyDefinition topology =
                topologyProperties.getTopologies().get(topologyVersion);
        if (topology == null || topology.getShards() == null || topology.getShards().isEmpty()) {
            throw new IllegalArgumentException("Unknown topology: " + topologyVersion);
        }
        return topology.getShards();
    }

    private int hashMod(Object shardKeyValue, int shardCount) {
        CRC32 crc = new CRC32();
        crc.update(String.valueOf(shardKeyValue).getBytes(StandardCharsets.UTF_8));
        return (int) Math.floorMod(crc.getValue(), shardCount);
    }
}
