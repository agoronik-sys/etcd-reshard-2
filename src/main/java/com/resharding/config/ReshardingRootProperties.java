package com.resharding.config;

import com.resharding.domain.PartitionStrategy;
import com.resharding.domain.RangeType;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Корневая конфигурация resharding из {@code application.yml}.
 *
 * <p>Объединяет topologies, tables и databaseLimits в одном бинe,
 * чтобы избежать конфликтов нескольких {@code @ConfigurationProperties} с пустым prefix.
 */
@Data
@ConfigurationProperties
public class ReshardingRootProperties {

    /** Карта topology: {@code v1} → shards [db1, db2], {@code v2} → [db1..db4]. */
    private Map<String, TopologyDefinition> topologies = new HashMap<>();

    /** Topology, в которой данные находятся на момент старта migration. */
    private String currentTopology;

    /** Целевая topology. */
    private String targetTopology;

    /** Конфигурация мигрируемых таблиц (shard key, range strategy, business key). */
    private Map<String, TableConfig> tables = new HashMap<>();

    /** Лимиты параллельных Task на каждый DB shard. */
    private Map<String, DatabaseLimit> databaseLimits = new HashMap<>();

    /** Описание одной topology. */
    @Data
    public static class TopologyDefinition {
        private String version;
        private List<String> shards;
        /** Точек на server для Ketama (nginx default 160). */
        private Integer ketamaPointsPerServer;
    }

    /** Параметры migration для одной таблицы. */
    @Data
    public static class TableConfig {

        /** Включена ли таблица в migration. По умолчанию {@code true}. */
        private boolean enabled = true;

        /** Имя таблицы в БД. Если не задано — используется ключ конфигурации. */
        private String tableName;

        /**
         * Правила выбора поля для расчёта hash.
         * Простой случай: {@code shardKey.field: customer_id}.
         */
        private ShardKeyConfig shardKey;

        /** {@link com.resharding.domain.ShardAlgorithm}: KETAMA (nginx) или HASH_MOD. */
        private String shardAlgorithm = "KETAMA";
        private RangeType rangeStrategy = RangeType.TIME;
        private String rangeColumn = "created_at";
        private String idColumn = "id";
        private List<String> businessKey;
        /** Override {@code migration.copy-batch-size} для таблицы; {@code 0} — глобальный. */
        private int batchSize = 0;

        private List<String> verifyDateColumns;
        private boolean verifyCompositeWithId = true;

        /**
         * Партиционирование по дате. Если включено — verify-индексы создаются
         * на физической партиции после полного залития месяца (только на новых shard).
         */
        private PartitionConfig partition;

        /** Возвращает имя таблицы в БД. */
        public String resolveTableName(String configKey) {
            if (tableName != null && !tableName.isBlank()) {
                return tableName;
            }
            return configKey;
        }

        /** Возвращает effective shard key config. */
        public ShardKeyConfig effectiveShardKey() {
            if (shardKey != null) {
                return shardKey;
            }
            throw new IllegalStateException(
                    "Table '%s' must define shardKey (field or defaultField + rules)"
                            .formatted(resolveTableName("?")));
        }

        public List<String> resolveVerifyDateColumns() {
            if (verifyDateColumns != null && !verifyDateColumns.isEmpty()) {
                return verifyDateColumns;
            }
            return List.of(rangeColumn);
        }

        /** Колонки business key для verify и проверки дублей перед INSERT. */
        public List<String> resolveBusinessKey() {
            if (businessKey == null || businessKey.isEmpty()) {
                throw new IllegalStateException(
                        "Table must define businessKey for existence check / verify");
            }
            return businessKey;
        }

        /** {@code true}, если таблица партиционирована по дате и индексы создаются по партициям. */
        public boolean isPartitioned() {
            return partition != null && partition.isEnabled();
        }
    }

    /**
     * Партиционирование таблицы по дате (обычно помесячно).
     *
     * <p>Verify-индексы на date-полях создаются на физической партиции
     * после того, как месяц полностью перенесён на новый shard — не на всю таблицу сразу.
     */
    @Data
    public static class PartitionConfig {

        /** Включено ли партиционирование для этой таблицы. */
        private boolean enabled = false;

        /** Стратегия: {@link PartitionStrategy#MONTHLY}. */
        private PartitionStrategy strategy = PartitionStrategy.MONTHLY;

        /**
         * Шаблон имени физической партиции.
         * Плейсхолдеры: {@code {table}}, {@code {yyyy}}, {@code {MM}}.
         */
        private String namePattern = "{table}_{yyyy}_{MM}";

        /**
         * Создавать verify-индексы только после полного залития месячной партиции.
         * По умолчанию {@code true}.
         */
        private boolean indexAfterPartitionComplete = true;
    }

    /**
     * Конфигурация поля (или полей) для расчёта hash.
     *
     * <p>Простой случай:
     * <pre>{@code shardKey: { field: customer_id }}</pre>
     *
     * <p>Условный случай (orders: если {@code eq = b12} → {@code mb_uid}, иначе {@code op_id}):
     * <pre>{@code
     * shardKey:
     *   defaultField: op_id
     *   rules:
     *     - whenField: eq
     *       whenValue: b12
     *       useField: mb_uid
     * }</pre>
     */
    @Data
    public static class ShardKeyConfig {

        /**
         * Поле, выбранное для hash, обязано глобально идентифицировать строку
         * среди всех source shard. Этот инвариант нужен migration-ledger:
         * одинаковые локальные PK на db1/db2 допустимы, но hash key — уникален.
         */
        private boolean unique = false;

        /** Одно поле для простого случая. */
        private String field;

        /** Поле по умолчанию, если ни одно правило не сработало. */
        private String defaultField;

        /** Условные правила выбора поля. */
        private List<ShardKeyRule> rules = List.of();

        public String resolveFieldName(Map<String, Object> rowValues) {
            if (field != null && !field.isBlank()) {
                return field;
            }
            if (rules != null) {
                for (ShardKeyRule rule : rules) {
                    if (rule.matches(rowValues)) {
                        return rule.getUseField();
                    }
                }
            }
            if (defaultField != null && !defaultField.isBlank()) {
                return defaultField;
            }
            throw new IllegalStateException("Shard key config must define field or defaultField");
        }

        public Set<String> requiredFields() {
            Set<String> fields = new LinkedHashSet<>();
            if (field != null && !field.isBlank()) {
                fields.add(field);
                return fields;
            }
            if (defaultField != null && !defaultField.isBlank()) {
                fields.add(defaultField);
            }
            if (rules != null) {
                for (ShardKeyRule rule : rules) {
                    fields.add(rule.getWhenField());
                    fields.add(rule.getUseField());
                }
            }
            return fields;
        }
    }

    /** Условное правило: если {@code whenField = whenValue}, hash считается по {@code useField}. */
    @Data
    public static class ShardKeyRule {

        /** Поле-дискриминатор, например {@code eq}. */
        private String whenField;

        /** Значение дискриминатора, например {@code b12}. */
        private String whenValue;

        /** Поле для hash, если правило сработало, например {@code mb_uid}. */
        private String useField;

        public boolean matches(Map<String, Object> rowValues) {
            if (whenField == null || whenValue == null) {
                return false;
            }
            Object actual = rowValues.get(whenField);
            return actual != null && whenValue.equals(String.valueOf(actual));
        }
    }

    /** Лимиты нагрузки на конкретный DB shard. */
    @Data
    public static class DatabaseLimit {
        private int maxSourceTasks = 2;
        private int maxTargetTasks = 2;
    }
}
