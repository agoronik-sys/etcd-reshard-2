package com.resharding.db;

import com.resharding.config.ReshardingRootProperties;
import org.junit.jupiter.api.Test;

import java.time.YearMonth;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет единое преобразование YearMonth в физическое имя партиции и etcd key.
 *
 * <p>Worker/Leader должны получить одно и то же имя; расхождение шаблона привело
 * бы к созданию индекса не на той таблице или к повторной index-операции.
 */
class PartitionNameResolverTest {

    private final PartitionNameResolver resolver = new PartitionNameResolver();

    @Test
    void resolvesDefaultMonthlyPartitionName() {
        ReshardingRootProperties.PartitionConfig config = new ReshardingRootProperties.PartitionConfig();
        config.setNamePattern("{table}_{yyyy}_{MM}");

        String name = resolver.resolve("orders", YearMonth.of(2024, 1), config);

        assertThat(name).isEqualTo("orders_2024_01");
    }

    @Test
    void partitionKeyUsesIsoFormat() {
        assertThat(resolver.partitionKey(YearMonth.of(2024, 3))).isEqualTo("2024-03");
    }
}
