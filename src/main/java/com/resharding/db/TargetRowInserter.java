package com.resharding.db;

import com.resharding.config.MigrationProperties;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Вставка строк на target shard.
 *
 * <p>Каждая запись защищена transactional migration-ledger по глобально
 * уникальному hash key. Это одинаково безопасно для первого прохода и reclaim.
 */
@Component
public class TargetRowInserter {

    private final DataSourceRegistry dataSourceRegistry;
    private final MigrationProperties migrationProperties;
    private final Set<String> initializedLedgers = ConcurrentHashMap.newKeySet();

    public TargetRowInserter(
            DataSourceRegistry dataSourceRegistry,
            MigrationProperties migrationProperties) {
        this.dataSourceRegistry = dataSourceRegistry;
        this.migrationProperties = migrationProperties;
    }

    /**
     * Идемпотентно переносит строку, используя глобально уникальное hash-поле.
     *
     * <p>Локальный PK source использовать нельзя: одинаковый {@code id=42} может
     * существовать на db1 и db2. Ledger идентифицирует логическую строку по
     * {@code table + shardKeyField + shardKeyValue}. Имя поля входит в ключ,
     * потому что условное правило может выбрать {@code op_id} или {@code mb_uid}.
     *
     * <p>Ledger INSERT и INSERT бизнес-строки выполняются в одной транзакции.
     * Если второй INSERT падает, ledger откатывается; при retry строка не будет
     * ошибочно считаться перенесённой.
     */
    public boolean insertWithLedger(
            String targetDbId,
            String migrationId,
            String table,
            String shardKeyField,
            Object shardKeyValue,
            Map<String, Object> row,
            String excludeColumn) {

        DataSource dataSource = dataSourceRegistry.get(targetDbId);
        ensureLedger(targetDbId, dataSource);

        List<String> columns = insertColumns(row, excludeColumn);
        String rowSql = buildInsertSql(table, columns);
        String ledgerSql = """
                INSERT INTO %s
                    (migration_id, source_table, shard_key_field, shard_key_value)
                VALUES (?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """.formatted(qualifiedLedgerTable());

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                int claimed;
                try (PreparedStatement ledger = conn.prepareStatement(ledgerSql)) {
                    ledger.setString(1, migrationId);
                    ledger.setString(2, table);
                    ledger.setString(3, shardKeyField);
                    ledger.setString(4, String.valueOf(shardKeyValue));
                    claimed = ledger.executeUpdate();
                }

                if (claimed == 0) {
                    conn.rollback();
                    return false;
                }

                try (PreparedStatement insert = conn.prepareStatement(rowSql)) {
                    int index = 1;
                    for (String column : columns) {
                        insert.setObject(index++, row.get(column));
                    }
                    insert.executeUpdate();
                }
                conn.commit();
                return true;
            } catch (Exception e) {
                rollbackQuietly(conn, e);
                throw e;
            }
        } catch (Exception e) {
            throw new TargetRowInsertException(
                    "Failed ledger insert on %s.%s".formatted(targetDbId, table), e);
        }
    }

    private String buildInsertSql(String table, List<String> columns) {
        String columnList = columns.stream().map(SqlIdentifiers::quote).reduce((a, b) -> a + ", " + b).orElse("");
        String placeholders = columns.stream().map(c -> "?").reduce((a, b) -> a + ", " + b).orElse("");
        return "INSERT INTO %s (%s) VALUES (%s)".formatted(
                SqlIdentifiers.quoteQualified(table), columnList, placeholders);
    }

    private List<String> insertColumns(Map<String, Object> row, String excludeColumn) {
        List<String> columns = new ArrayList<>();
        for (String col : row.keySet()) {
            if (!col.equals(excludeColumn)) {
                columns.add(col);
            }
        }
        return columns;
    }

    private void ensureLedger(String targetDbId, DataSource dataSource) {
        if (!migrationProperties.getLedger().isAutoCreate()
                || initializedLedgers.contains(targetDbId)) {
            return;
        }

        String sql = """
                CREATE TABLE IF NOT EXISTS %s (
                    migration_id TEXT NOT NULL,
                    source_table TEXT NOT NULL,
                    shard_key_field TEXT NOT NULL,
                    shard_key_value TEXT NOT NULL,
                    transferred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (migration_id, source_table, shard_key_field, shard_key_value)
                )
                """.formatted(qualifiedLedgerTable());
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.execute();
            initializedLedgers.add(targetDbId);
        } catch (SQLException e) {
            throw new TargetRowInsertException(
                    "Failed to initialize migration ledger on " + targetDbId, e);
        }
    }

    private String qualifiedLedgerTable() {
        MigrationProperties.Ledger ledger = migrationProperties.getLedger();
        return SqlIdentifiers.quote(ledger.getSchema()) + "." + SqlIdentifiers.quote(ledger.getTableName());
    }

    private void rollbackQuietly(Connection conn, Exception original) {
        try {
            conn.rollback();
        } catch (SQLException rollbackError) {
            original.addSuppressed(rollbackError);
        }
    }
}
