package com.resharding.migration.ketama;

import com.resharding.config.ReshardingRootProperties;
import com.resharding.config.SegmentsProperties;
import com.resharding.config.TopologyProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Кэш Ketama ring по версии topology.
 *
 * <p>Ring строится из списка shard target topology. Имя server в ring берётся из
 * {@code segments.database.{dbId}.ketamaServer} — должно совпадать с nginx upstream
 * (например, {@code host:port}). Если не задано — используется {@code dbId}.
 */
@Component
public class KetamaRingFactory {

    private static final int DEFAULT_POINTS_PER_SERVER = 160;

    private final TopologyProperties topologyProperties;
    private final SegmentsProperties segmentsProperties;
    private final Map<String, NginxKetamaHashRing> cache = new ConcurrentHashMap<>();

    public KetamaRingFactory(TopologyProperties topologyProperties, SegmentsProperties segmentsProperties) {
        this.topologyProperties = topologyProperties;
        this.segmentsProperties = segmentsProperties;
    }

    public NginxKetamaHashRing getRing(String topologyVersion) {
        return cache.computeIfAbsent(topologyVersion, this::buildRing);
    }

    public void invalidateAll() {
        cache.clear();
    }

    private NginxKetamaHashRing buildRing(String topologyVersion) {
        ReshardingRootProperties.TopologyDefinition topology =
                topologyProperties.getTopologies().get(topologyVersion);
        if (topology == null || topology.getShards() == null || topology.getShards().isEmpty()) {
            throw new IllegalArgumentException("Unknown topology: " + topologyVersion);
        }

        List<String> shards = topology.getShards();
        int n = shards.size();
        String[] nodeIds = shards.toArray(new String[0]);
        String[] ketamaNames = new String[n];
        int[] weights = new int[n];

        int pointsPerServer = topology.getKetamaPointsPerServer() != null
                ? topology.getKetamaPointsPerServer()
                : DEFAULT_POINTS_PER_SERVER;

        for (int i = 0; i < n; i++) {
            String dbId = shards.get(i);
            SegmentsProperties.DatabaseConnectionProperties dbConfig =
                    segmentsProperties.getDatabase().get(dbId);
            ketamaNames[i] = resolveKetamaServer(dbId, dbConfig);
            weights[i] = dbConfig != null && dbConfig.getKetamaWeight() > 0
                    ? dbConfig.getKetamaWeight()
                    : 1;
        }

        return NginxKetamaHashRing.build(nodeIds, ketamaNames, weights, pointsPerServer);
    }

    private String resolveKetamaServer(String dbId, SegmentsProperties.DatabaseConnectionProperties dbConfig) {
        if (dbConfig != null && dbConfig.getKetamaServer() != null && !dbConfig.getKetamaServer().isBlank()) {
            return dbConfig.getKetamaServer();
        }
        return dbId;
    }
}
