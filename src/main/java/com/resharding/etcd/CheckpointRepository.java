package com.resharding.etcd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.resharding.domain.IncrementalCheckpoint;
import com.resharding.domain.TableCheckpoint;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Репозиторий checkpoint bulk и incremental migration.
 *
 * <p>Bulk checkpoint изменяется только Leader'ом через CAS ({@link #updateBulkCheckpoint}).
 * Incremental checkpoint ведёт отдельный поток обработки данных после {@code T0}.
 */
@Repository
public class CheckpointRepository {

    private final EtcdClientFacade etcd;
    private final EtcdKeyPaths keys;
    private final ObjectMapper objectMapper;

    public CheckpointRepository(EtcdClientFacade etcd, EtcdKeyPaths keys, ObjectMapper objectMapper) {
        this.etcd = etcd;
        this.keys = keys;
        this.objectMapper = objectMapper;
    }

    /** Читает bulk checkpoint пары (таблица, source shard) вместе с revision для CAS. */
    public Optional<TableCheckpoint> getBulkCheckpoint(String migrationId, String table, String sourceDb) {
        return etcd.getWithRevision(keys.checkpoint(migrationId, table, sourceDb))
                .map(entry -> parseCheckpoint(entry, table, sourceDb));
    }

    /**
     * Атомарно обновляет bulk checkpoint при совпадении revision.
     *
     * @return {@code false} если revision изменилась (конкурентное обновление)
     */
    public boolean updateBulkCheckpoint(String migrationId, TableCheckpoint checkpoint, long expectedRevision) {
        return etcd.compareAndSwap(
                keys.checkpoint(migrationId, checkpoint.getTable(), checkpoint.getSourceDb()),
                expectedRevision,
                checkpoint);
    }

    /** Создаёт начальный bulk checkpoint, только если его ещё нет. */
    public boolean initBulkCheckpoint(String migrationId, TableCheckpoint checkpoint) {
        return etcd.putIfAbsent(
                keys.checkpoint(migrationId, checkpoint.getTable(), checkpoint.getSourceDb()),
                checkpoint,
                null);
    }

    /** Читает incremental checkpoint таблицы. */
    public Optional<IncrementalCheckpoint> getIncrementalCheckpoint(String migrationId, String table) {
        return etcd.get(keys.incrementalCheckpoint(migrationId, table), IncrementalCheckpoint.class);
    }

    /** Сохраняет incremental checkpoint таблицы. */
    public void saveIncrementalCheckpoint(String migrationId, String table, IncrementalCheckpoint checkpoint) {
        etcd.put(keys.incrementalCheckpoint(migrationId, table), checkpoint);
    }

    private TableCheckpoint parseCheckpoint(EtcdClientFacade.EtcdEntry entry, String table, String sourceDb) {
        try {
            TableCheckpoint checkpoint = objectMapper.readValue(entry.json(), TableCheckpoint.class);
            checkpoint.setRevision(entry.revision());
            if (checkpoint.getTable() == null) {
                checkpoint.setTable(table);
            }
            if (checkpoint.getSourceDb() == null) {
                checkpoint.setSourceDb(sourceDb);
            }
            return checkpoint;
        } catch (Exception e) {
            throw new EtcdOperationException("Failed to parse checkpoint for table " + table, e);
        }
    }
}
