package com.resharding.config;

import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Доступ к лимитам нагрузки на DB ({@code databaseLimits.*}).
 */
@Component
public class DatabaseLimitsProperties {

    private final ReshardingRootProperties root;

    public DatabaseLimitsProperties(ReshardingRootProperties root) {
        this.root = root;
    }

    public Map<String, ReshardingRootProperties.DatabaseLimit> getLimits() {
        return root.getDatabaseLimits();
    }
}
