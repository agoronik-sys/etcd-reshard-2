package com.resharding.etcd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.resharding.domain.ActiveMigrationLock;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Репозиторий блокировки единственной активной migration.
 *
 * <p>Гарантирует инвариант «только одна RUNNING migration» через
 * atomic {@code put-if-absent} по ключу {@link EtcdKeyPaths#migrationActiveLock()}.
 */
@Repository
public class ActiveMigrationLockRepository {

    private final EtcdClientFacade etcd;
    private final EtcdKeyPaths keys;
    private final ObjectMapper objectMapper;

    public ActiveMigrationLockRepository(
            EtcdClientFacade etcd, EtcdKeyPaths keys, ObjectMapper objectMapper) {
        this.etcd = etcd;
        this.keys = keys;
        this.objectMapper = objectMapper;
    }

    /** Возвращает текущую блокировку, если она существует. */
    public Optional<ActiveMigrationLock> get() {
        return etcd.get(keys.migrationActiveLock(), ActiveMigrationLock.class);
    }

    public Optional<VersionedLock> getVersioned() {
        return etcd.getWithRevision(keys.migrationActiveLock())
                .map(entry -> {
                    try {
                        return new VersionedLock(
                                objectMapper.readValue(entry.json(), ActiveMigrationLock.class),
                                entry.revision());
                    } catch (Exception e) {
                        throw new EtcdOperationException("Failed to parse active migration lock", e);
                    }
                });
    }

    /**
     * Пытается захватить блокировку для новой migration.
     *
     * @return {@code true} если блокировка успешно создана
     */
    public boolean tryAcquire(String migrationId, String owner, long leaderLeaseId) {
        ActiveMigrationLock lock = ActiveMigrationLock.builder()
                .migrationId(migrationId)
                .owner(owner)
                .build();
        return etcd.putIfAbsent(keys.migrationActiveLock(), lock, leaderLeaseId);
    }

    /**
     * Освобождает блокировку, если она принадлежит указанному owner.
     * Безопасен при повторном вызове или несовпадении owner.
     */
    public boolean release(String migrationId) {
        Optional<EtcdClientFacade.EtcdEntry> entry = etcd.getWithRevision(keys.migrationActiveLock());
        if (entry.isEmpty()) {
            return true;
        }
        ActiveMigrationLock existing;
        try {
            existing = objectMapper.readValue(entry.get().json(), ActiveMigrationLock.class);
        } catch (Exception e) {
            throw new EtcdOperationException("Failed to parse active migration lock", e);
        }
        if (!existing.getMigrationId().equals(migrationId)) {
            return false;
        }
        return etcd.compareAndDelete(keys.migrationActiveLock(), entry.get().revision());
    }

    public record VersionedLock(ActiveMigrationLock lock, long revision) {}
}
