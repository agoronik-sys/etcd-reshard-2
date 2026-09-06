package com.resharding.etcd;

import com.resharding.domain.TableIndexStatus;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Репозиторий статуса verify-индексов на новых DB shard.
 *
 * <p>Поддерживает как индексы на всю таблицу, так и на отдельные партиции
 * ({@code partitionKey = 2024-01}).
 */
@Repository
public class TableIndexStatusRepository {

    private final EtcdClientFacade etcd;
    private final EtcdKeyPaths keys;

    public TableIndexStatusRepository(EtcdClientFacade etcd, EtcdKeyPaths keys) {
        this.etcd = etcd;
        this.keys = keys;
    }

    public Optional<TableIndexStatus> get(String migrationId, String dbId, String table) {
        return etcd.get(keys.tableIndexStatus(migrationId, dbId, table), TableIndexStatus.class);
    }

    public Optional<TableIndexStatus> getPartition(
            String migrationId, String dbId, String table, String partitionKey) {
        return etcd.get(
                keys.tablePartitionIndexStatus(migrationId, dbId, table, partitionKey),
                TableIndexStatus.class);
    }

    public void save(TableIndexStatus status) {
        String key = status.getPartitionKey() != null
                ? keys.tablePartitionIndexStatus(
                        status.getMigrationId(), status.getDbId(), status.getTable(), status.getPartitionKey())
                : keys.tableIndexStatus(status.getMigrationId(), status.getDbId(), status.getTable());
        etcd.put(key, status);
    }

    public boolean isCreated(String migrationId, String dbId, String table) {
        return get(migrationId, dbId, table)
                .map(s -> s.getStatus() == TableIndexStatus.IndexStatus.CREATED)
                .orElse(false);
    }

    public boolean isPartitionCreated(String migrationId, String dbId, String table, String partitionKey) {
        return getPartition(migrationId, dbId, table, partitionKey)
                .map(s -> s.getStatus() == TableIndexStatus.IndexStatus.CREATED)
                .orElse(false);
    }
}
