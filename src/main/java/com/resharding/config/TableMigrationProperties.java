package com.resharding.config;

import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Доступ к конфигурации мигрируемых таблиц ({@code tables.*}).
 */
@Component
public class TableMigrationProperties {

    private final ReshardingRootProperties root;

    public TableMigrationProperties(ReshardingRootProperties root) {
        this.root = root;
    }

    public Map<String, ReshardingRootProperties.TableConfig> getTables() {
        return root.getTables();
    }

    /**
     * Только включённые таблицы ({@code enabled: true}).
     * Ключ map — id конфигурации; {@link ReshardingRootProperties.TableConfig#getTableName()} — имя в БД.
     */
    public Map<String, ReshardingRootProperties.TableConfig> getEnabledTables() {
        return root.getTables().entrySet().stream()
                .filter(e -> e.getValue().isEnabled())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> a,
                        LinkedHashMap::new));
    }

    public ReshardingRootProperties.TableConfig getEnabledTable(String configKey) {
        ReshardingRootProperties.TableConfig config = root.getTables().get(configKey);
        if (config == null || !config.isEnabled()) {
            return null;
        }
        return config;
    }
}
