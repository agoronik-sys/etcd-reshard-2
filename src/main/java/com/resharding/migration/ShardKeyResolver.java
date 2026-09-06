package com.resharding.migration;

import com.resharding.config.ReshardingRootProperties;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Выбор поля shard key для расчёта target shard по конфигурации таблицы.
 *
 * <p>Поддерживает:
 * <ul>
 *   <li>простой случай — одно поле ({@code shardKey.field});</li>
 *   <li>условный случай — выбор поля по значению другого поля
 *       (например, {@code eq = b12 → mb_uid}, иначе {@code op_id}).</li>
 * </ul>
 */
@Component
public class ShardKeyResolver {

    /**
     * Определяет имя поля, по которому нужно считать hash для строки.
     */
    public String resolveShardKeyField(
            ReshardingRootProperties.TableConfig tableConfig,
            Map<String, Object> rowValues) {

        ReshardingRootProperties.ShardKeyConfig shardKey = tableConfig.getShardKey();
        if (shardKey == null) {
            throw new IllegalStateException("Shard key config is not defined for table " + tableConfig.getTableName());
        }
        return shardKey.resolveFieldName(rowValues);
    }

    /**
     * Извлекает значение shard key из строки с учётом условных правил.
     */
    public Object resolveShardKeyValue(
            ReshardingRootProperties.TableConfig tableConfig,
            Map<String, Object> rowValues) {

        String fieldName = resolveShardKeyField(tableConfig, rowValues);
        if (!rowValues.containsKey(fieldName)) {
            throw new IllegalStateException(
                    "Row does not contain shard key field '%s' for table %s"
                            .formatted(fieldName, tableConfig.getTableName()));
        }
        return rowValues.get(fieldName);
    }

    /**
     * Все колонки, которые нужно прочитать из source для расчёта shard key и batch-обработки.
     */
    public Set<String> requiredSelectColumns(ReshardingRootProperties.TableConfig tableConfig) {
        Set<String> columns = new LinkedHashSet<>();
        columns.add(tableConfig.getIdColumn());
        columns.add(tableConfig.getRangeColumn());

        ReshardingRootProperties.ShardKeyConfig shardKey = tableConfig.getShardKey();
        if (shardKey != null) {
            columns.addAll(shardKey.requiredFields());
        }

        if (tableConfig.getBusinessKey() != null) {
            columns.addAll(tableConfig.getBusinessKey());
        }

        return columns;
    }
}
