package com.resharding.db;

import com.resharding.config.MigrationProperties;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Быстрые unit-тесты JDBC orchestration без Docker.
 *
 * <p>Проверяют порядок действий внутри одной Connection: сначала ledger claim,
 * затем business INSERT и commit. Отдельный сценарий подтверждает, что конфликт
 * ledger не выполняет второй INSERT. Реальные ограничения PostgreSQL покрывает
 * {@link TargetRowLedgerIntegrationTest}.
 */
class TargetRowLedgerTest {

    @Test
    void ledgerAndBusinessRowCommitInOneTransaction() throws Exception {
        DataSourceRegistry registry = mock(DataSourceRegistry.class);
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement ledger = mock(PreparedStatement.class);
        PreparedStatement rowInsert = mock(PreparedStatement.class);
        when(registry.get("db3")).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(ledger, rowInsert);
        when(ledger.executeUpdate()).thenReturn(1);

        MigrationProperties properties = new MigrationProperties();
        properties.getLedger().setAutoCreate(false);
        TargetRowInserter inserter = new TargetRowInserter(registry, properties);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 7L);
        row.put("op_id", "global-42");
        row.put("payload", "value");

        assertThat(inserter.insertWithLedger(
                "db3", "mig-1", "orders", "op_id", "global-42", row, "id")).isTrue();

        var order = inOrder(connection, ledger, rowInsert);
        order.verify(connection).setAutoCommit(false);
        order.verify(ledger).executeUpdate();
        order.verify(rowInsert).executeUpdate();
        order.verify(connection).commit();
    }

    @Test
    void duplicateHashKeySkipsBusinessInsert() throws Exception {
        DataSourceRegistry registry = mock(DataSourceRegistry.class);
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement ledger = mock(PreparedStatement.class);
        when(registry.get("db4")).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(ledger);
        when(ledger.executeUpdate()).thenReturn(0);

        MigrationProperties properties = new MigrationProperties();
        properties.getLedger().setAutoCreate(false);
        TargetRowInserter inserter = new TargetRowInserter(registry, properties);

        assertThat(inserter.insertWithLedger(
                "db4", "mig-1", "orders", "op_id", "global-42",
                Map.of("id", 7L, "op_id", "global-42"), "id")).isFalse();

        verify(connection).rollback();
    }
}
