package com.resharding.migration;

import com.resharding.config.ReshardingRootProperties;
import com.resharding.config.TopologyProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Определяет DB shard'ы, появившиеся только в target topology.
 *
 * <p>Пример: {@code v1 = [db1, db2]}, {@code v2 = [db1, db2, db3, db4]}
 * → новые инстансы: {@code db3}, {@code db4}.
 *
 * <p>Индексы для verify создаются только на новых инстансах; существующие shard'ы
 * уже содержат необходимые индексы.
 */
@Component
public class NewShardResolver {

    private final TopologyProperties topologyProperties;

    public NewShardResolver(TopologyProperties topologyProperties) {
        this.topologyProperties = topologyProperties;
    }

    /**
     * Возвращает shard'ы, присутствующие в target topology, но отсутствующие в current.
     *
     * @param currentTopologyVersion версия исходной topology (например, {@code v1})
     * @param targetTopologyVersion  версия целевой topology (например, {@code v2})
     */
    public List<String> resolveNewShards(String currentTopologyVersion, String targetTopologyVersion) {
        Set<String> current = new HashSet<>(shardsOf(currentTopologyVersion));
        List<String> target = shardsOf(targetTopologyVersion);
        List<String> newShards = new ArrayList<>();
        for (String shard : target) {
            if (!current.contains(shard)) {
                newShards.add(shard);
            }
        }
        return newShards;
    }

    /**
     * {@code true} если shard появился только в target topology.
     */
    public boolean isNewShard(String dbId, String currentTopologyVersion, String targetTopologyVersion) {
        return resolveNewShards(currentTopologyVersion, targetTopologyVersion).contains(dbId);
    }

    private List<String> shardsOf(String topologyVersion) {
        ReshardingRootProperties.TopologyDefinition topology =
                topologyProperties.getTopologies().get(topologyVersion);
        if (topology == null || topology.getShards() == null) {
            throw new IllegalArgumentException("Unknown topology: " + topologyVersion);
        }
        return topology.getShards();
    }
}
