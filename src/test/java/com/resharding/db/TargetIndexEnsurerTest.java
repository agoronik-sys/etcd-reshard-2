package com.resharding.db;

import com.resharding.config.ReshardingRootProperties;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Регрессия для PostgreSQL DDL verify-индексов.
 *
 * <p>Длинные имена месячных партиций PostgreSQL молча усекает до 63 байт,
 * что может создать коллизию. Тест также фиксирует обязательное quoting имён
 * таблиц и колонок, формируемых из конфигурации.
 */
class TargetIndexEnsurerTest {

    private final TargetIndexEnsurer ensurer =
            new TargetIndexEnsurer(null, null, null, null, null);

    @Test
    void quotesIdentifiersAndLimitsGeneratedIndexName() {
        ReshardingRootProperties.TableConfig table = new ReshardingRootProperties.TableConfig();
        table.setIdColumn("id");
        table.setVerifyDateColumns(List.of("created_at"));
        table.setVerifyCompositeWithId(true);
        String longPartition = "orders_" + "very_long_partition_name_".repeat(4);

        List<String> statements = ensurer.buildIndexStatements(longPartition, table);

        assertThat(statements).hasSize(2);
        for (String sql : statements) {
            Matcher matcher = Pattern.compile("IF NOT EXISTS \"([^\"]+)\"").matcher(sql);
            assertThat(matcher.find()).isTrue();
            assertThat(matcher.group(1).length()).isLessThanOrEqualTo(63);
            assertThat(sql).contains("ON \"" + longPartition + "\"");
            assertThat(sql).contains("\"created_at\"");
        }
    }
}
