package com.resharding.db;

import com.resharding.config.SegmentsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Проверяет lifecycle ленивых Hikari pools без реального подключения к БД.
 *
 * <p>Основной инвариант: старт Pod не создаёт pools, доступ без активной migration
 * запрещён, а STOP не закрывает pool посередине pinned JDBC-операции.
 */
class DataSourceRegistryTest {

    private DataSourceRegistry registry;

    @BeforeEach
    void setUp() {
        SegmentsProperties properties = new SegmentsProperties();
        SegmentsProperties.DatabaseConnectionProperties db1 = new SegmentsProperties.DatabaseConnectionProperties();
        db1.setUrl("jdbc:postgresql://localhost:5432/db1");
        db1.setUsername("user");
        db1.setPassword("pass");
        properties.setDatabase(Map.of("db1", db1));

        registry = new DataSourceRegistry(properties);
    }

    @Test
    void doesNotOpenPoolsOnConstruction() {
        assertThat(registry.isActive()).isFalse();
        assertThat(registry.openPools()).isEmpty();
    }

    @Test
    void refusesConnectionWithoutActiveMigration() {
        assertThatThrownBy(() -> registry.get("db1"))
                .isInstanceOf(DataSourceInactiveException.class)
                .hasMessageContaining("no active migration");

        assertThat(registry.openPools()).isEmpty();
    }

    @Test
    void rejectsUnknownShardEvenWhenActive() {
        registry.activate();

        assertThatThrownBy(() -> registry.get("db99"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("db99");
    }

    @Test
    void deactivateIsIdempotentWhenNothingWasOpened() {
        registry.activate();
        registry.deactivate();
        registry.deactivate();

        assertThat(registry.isActive()).isFalse();
        assertThat(registry.openPools()).isEmpty();
    }

    @Test
    void deactivateWaitsForPinnedOperation() {
        registry.activate();
        registry.beginOperation();

        registry.deactivate();
        assertThat(registry.isActive()).isTrue();

        registry.endOperation();
        assertThat(registry.isActive()).isFalse();
    }

    @Test
    void refusesNewOperationAfterDeactivate() {
        registry.activate();
        registry.deactivate();

        assertThatThrownBy(registry::beginOperation)
                .isInstanceOf(DataSourceInactiveException.class);
    }
}
