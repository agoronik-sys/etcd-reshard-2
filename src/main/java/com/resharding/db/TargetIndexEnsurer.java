package com.resharding.db;



import com.resharding.config.ReshardingRootProperties;

import com.resharding.config.TableMigrationProperties;

import com.resharding.domain.MigrationState;

import com.resharding.domain.MigrationTask;

import com.resharding.domain.TableIndexStatus;

import com.resharding.etcd.TableIndexStatusRepository;

import com.resharding.migration.NewShardResolver;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;



import javax.sql.DataSource;

import java.sql.Connection;

import java.sql.SQLException;

import java.sql.Statement;

import java.time.LocalDateTime;

import java.time.YearMonth;

import java.nio.charset.StandardCharsets;

import java.security.MessageDigest;

import java.security.NoSuchAlgorithmException;

import java.util.ArrayList;

import java.util.List;



/**

 * Создание verify-индексов на date-полях для сверки после копирования.

 *

 * <h3>Непартиционированные таблицы</h3>

 * Индексы создаются лениво после появления Task (перед verify) —

 * только на <strong>новых</strong> DB shard ({@link NewShardResolver}).

 *

 * <h3>Партиционированные по дате таблицы</h3>

 * Индексы создаются на <em>физической партиции</em> (например, {@code orders_2024_01})

 * после того, как календарный месяц полностью залит на новый shard.

 * До завершения месяца verify по date-полям откладывается; per-batch сверка

 * возможна по {@code businessKey}.

 *

 * <p>Pipeline Worker для партиционированной таблицы:

 * <pre>

 * READ source → WRITE target → COMMIT target

 *   (пока месяц не залит полностью — verify по businessKey)

 * Leader: месяц завершён → CREATE INDEX ON partition (новые shard)

 *   → VERIFY partition по date-полям

 *   → DELETE source

 * </pre>

 */

@Slf4j

@Service

public class TargetIndexEnsurer {



    private final DataSourceRegistry dataSourceRegistry;

    private final NewShardResolver newShardResolver;

    private final TableMigrationProperties tableMigrationProperties;

    private final TableIndexStatusRepository indexStatusRepository;

    private final PartitionNameResolver partitionNameResolver;



    public TargetIndexEnsurer(

            DataSourceRegistry dataSourceRegistry,

            NewShardResolver newShardResolver,

            TableMigrationProperties tableMigrationProperties,

            TableIndexStatusRepository indexStatusRepository,

            PartitionNameResolver partitionNameResolver) {

        this.dataSourceRegistry = dataSourceRegistry;

        this.newShardResolver = newShardResolver;

        this.tableMigrationProperties = tableMigrationProperties;

        this.indexStatusRepository = indexStatusRepository;

        this.partitionNameResolver = partitionNameResolver;

    }



    /**

     * Создаёт verify-индексы на всех новых shard для <em>непартиционированных</em> таблиц.

     * Партиционированные таблицы пропускаются — индексы создаются помесячно.

     */

    public void ensureIndexesForNewShards(MigrationState migration) {

        List<String> newShards = newShardResolver.resolveNewShards(

                migration.getCurrentTopology(),

                migration.getTargetTopology());



        if (newShards.isEmpty()) {

            log.info("No new shards in migration {}; index creation skipped", migration.getMigrationId());

            return;

        }



        log.info("Ensuring verify indexes on new shards {} for migration {}",

                newShards, migration.getMigrationId());



        for (String dbId : newShards) {

            for (var entry : tableMigrationProperties.getEnabledTables().entrySet()) {

                ReshardingRootProperties.TableConfig tableConfig = entry.getValue();

                if (tableConfig.isPartitioned()) {

                    log.debug("Table {} is partitioned; whole-table indexes deferred until month complete",

                            entry.getValue().resolveTableName(entry.getKey()));

                    continue;

                }

                String tableName = entry.getValue().resolveTableName(entry.getKey());

                ensureTableIndexes(migration.getMigrationId(), dbId, tableName, tableConfig);

            }

        }

    }



    /**

     * Создаёт verify-индексы на физической партиции для всех новых shard

     * после полного залития календарного месяца.

     */

    public void ensurePartitionIndexesForCompletedMonth(

            String migrationId,

            String currentTopology,

            String targetTopology,

            MigrationTask task,

            ReshardingRootProperties.TableConfig tableConfig,

            YearMonth completedMonth) {



        if (!tableConfig.isPartitioned()) {

            return;

        }



        ReshardingRootProperties.PartitionConfig partitionConfig = tableConfig.getPartition();

        if (partitionConfig != null && !partitionConfig.isIndexAfterPartitionComplete()) {

            return;

        }



        List<String> newShards = newShardResolver.resolveNewShards(

                currentTopology,

                targetTopology);



        String parentTable = task.getTable();

        String partitionTable = partitionNameResolver.resolve(parentTable, completedMonth, partitionConfig);

        String partitionKey = partitionNameResolver.partitionKey(completedMonth);



        log.info("Month {} complete for {}.{}; creating partition indexes on {}",

                completedMonth, migrationId, parentTable, newShards);



        for (String dbId : newShards) {

            ensurePartitionIndexes(

                    migrationId,

                    dbId,

                    parentTable,

                    partitionTable,

                    partitionKey,

                    tableConfig);

        }

    }



    /**

     * Гарантирует наличие verify-индексов перед сверкой batch.

     *

     * <p>Для партиционированных таблиц — только если индексы на партицию месяца уже созданы.

     * Иначе date-verify пропускается (месяц ещё не залит полностью).

     */

    public boolean ensureBeforeVerify(

            String migrationId,

            String targetDbId,

            String table,

            ReshardingRootProperties.TableConfig tableConfig,

            LocalDateTime rowDate,

            String currentTopology,

            String targetTopology) {



        if (!newShardResolver.isNewShard(targetDbId, currentTopology, targetTopology)) {

            return true;

        }



        if (tableConfig.isPartitioned()) {

            YearMonth month = YearMonth.from(rowDate);

            String partitionKey = partitionNameResolver.partitionKey(month);

            return indexStatusRepository.isPartitionCreated(migrationId, targetDbId, table, partitionKey);

        }



        ensureTableIndexes(migrationId, targetDbId, table, tableConfig);

        return true;

    }



    private void ensureTableIndexes(

            String migrationId,

            String dbId,

            String table,

            ReshardingRootProperties.TableConfig tableConfig) {



        if (indexStatusRepository.isCreated(migrationId, dbId, table)) {

            return;

        }

        executeIndexCreation(migrationId, dbId, table, null, table, tableConfig);

    }



    private void ensurePartitionIndexes(

            String migrationId,

            String dbId,

            String parentTable,

            String partitionTable,

            String partitionKey,

            ReshardingRootProperties.TableConfig tableConfig) {



        if (indexStatusRepository.isPartitionCreated(migrationId, dbId, parentTable, partitionKey)) {

            return;

        }

        executeIndexCreation(migrationId, dbId, parentTable, partitionKey, partitionTable, tableConfig);

    }



    private void executeIndexCreation(

            String migrationId,

            String dbId,

            String logicalTable,

            String partitionKey,

            String physicalTable,

            ReshardingRootProperties.TableConfig tableConfig) {



        List<String> indexSqlList = buildIndexStatements(physicalTable, tableConfig);

        List<String> createdNames = new ArrayList<>();



        try {

            DataSource dataSource = dataSourceRegistry.get(dbId);

            try (Connection conn = dataSource.getConnection();

                 Statement stmt = conn.createStatement()) {

                for (String sql : indexSqlList) {

                    log.info("Creating verify index on {}.{}: {}", dbId, physicalTable, sql);

                    stmt.execute(sql);

                    createdNames.add(extractIndexName(sql));

                }

            }



            saveStatus(migrationId, dbId, logicalTable, partitionKey, createdNames, TableIndexStatus.IndexStatus.CREATED);

            log.info("Verify indexes created on {}.{}: {}", dbId, physicalTable, createdNames);



        } catch (SQLException e) {

            saveStatus(migrationId, dbId, logicalTable, partitionKey, createdNames, TableIndexStatus.IndexStatus.FAILED);

            throw new IndexCreationException(

                    "Failed to create verify indexes on " + dbId + "." + physicalTable, e);

        }

    }



    /**

     * Формирует DDL для индексов по date-полям на указанной физической таблице/партиции.

     */

    List<String> buildIndexStatements(String physicalTable, ReshardingRootProperties.TableConfig tableConfig) {

        List<String> columns = tableConfig.resolveVerifyDateColumns();

        String idColumn = tableConfig.getIdColumn();

        List<String> statements = new ArrayList<>();



        for (String dateColumn : columns) {

            String indexName = postgresIndexName("idx_%s_verify_%s".formatted(physicalTable, dateColumn));

            statements.add("""

                    CREATE INDEX IF NOT EXISTS %s ON %s (%s)

                    """.formatted(
                    SqlIdentifiers.quote(indexName),
                    SqlIdentifiers.quoteQualified(physicalTable),
                    SqlIdentifiers.quote(dateColumn)).trim());



            if (tableConfig.isVerifyCompositeWithId()) {

                String compositeName = postgresIndexName(
                        "idx_%s_verify_%s_%s".formatted(physicalTable, dateColumn, idColumn));

                statements.add("""

                        CREATE INDEX IF NOT EXISTS %s ON %s (%s, %s)

                        """.formatted(
                        SqlIdentifiers.quote(compositeName),
                        SqlIdentifiers.quoteQualified(physicalTable),
                        SqlIdentifiers.quote(dateColumn),
                        SqlIdentifiers.quote(idColumn)).trim());

            }

        }



        return statements;

    }

    /**
     * PostgreSQL ограничивает identifier 63 байтами. Простое усечение имён
     * партиций создаёт коллизии, поэтому длинное имя получает стабильный hash.
     */
    private String postgresIndexName(String requested) {
        String safe = requested.replaceAll("[^A-Za-z0-9_$]", "_");
        if (safe.getBytes(StandardCharsets.UTF_8).length <= 63) {
            return safe;
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(safe.getBytes(StandardCharsets.UTF_8));
            String hash = java.util.HexFormat.of().formatHex(digest, 0, 6);
            return safe.substring(0, 63 - hash.length() - 1) + "_" + hash;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the JDK", e);
        }
    }



    private void saveStatus(

            String migrationId,

            String dbId,

            String table,

            String partitionKey,

            List<String> indexNames,

            TableIndexStatus.IndexStatus status) {



        indexStatusRepository.save(TableIndexStatus.builder()

                .migrationId(migrationId)

                .dbId(dbId)

                .table(table)

                .partitionKey(partitionKey)

                .indexNames(indexNames)

                .status(status)

                .createdAt(LocalDateTime.now())

                .build());

    }



    private String extractIndexName(String createIndexSql) {

        int start = createIndexSql.indexOf("IF NOT EXISTS") + "IF NOT EXISTS".length();

        int end = createIndexSql.indexOf(" ON ");

        return createIndexSql.substring(start, end).trim().replace("\"", "");

    }

}

