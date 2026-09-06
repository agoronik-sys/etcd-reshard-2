package com.resharding.config;

import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Доступ к конфигурации topologies ({@code currentTopology}, {@code targetTopology}).
 *
 * <p>Делегирует в {@link ReshardingRootProperties}.
 */
@Component
public class TopologyProperties {

    private final ReshardingRootProperties root;

    public TopologyProperties(ReshardingRootProperties root) {
        this.root = root;
    }

    public Map<String, ReshardingRootProperties.TopologyDefinition> getTopologies() {
        return root.getTopologies();
    }

    public String getCurrentTopology() {
        return root.getCurrentTopology();
    }

    public String getTargetTopology() {
        return root.getTargetTopology();
    }
}
