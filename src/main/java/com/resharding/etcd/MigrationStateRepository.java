package com.resharding.etcd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationStatus;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Репозиторий текущего состояния migration в {@code etcd}.
 *
 * <p>Ключ: {@link EtcdKeyPaths#migrationCurrent()}.
 */
@Repository
public class MigrationStateRepository {

    private final EtcdClientFacade etcd;
    private final EtcdKeyPaths keys;
    private final ObjectMapper objectMapper;

    public MigrationStateRepository(EtcdClientFacade etcd, EtcdKeyPaths keys, ObjectMapper objectMapper) {
        this.etcd = etcd;
        this.keys = keys;
        this.objectMapper = objectMapper;
    }

    /** Возвращает текущую migration или {@link Optional#empty()}. */
    public Optional<MigrationState> getCurrent() {
        return etcd.get(keys.migrationCurrent(), MigrationState.class);
    }

    /** Сохраняет (перезаписывает) состояние текущей migration. */
    public void save(MigrationState state) {
        etcd.put(keys.migrationCurrent(), state);
    }

    /**
     * Обновляет статус migration при совпадении ожидаемого состояния.
     *
     * @return {@code false} если текущий статус не совпадает с {@code expected}
     */
    public boolean updateStatus(MigrationState state, MigrationStatus expected, MigrationStatus newStatus) {
        if (state.getStatus() != expected) {
            return false;
        }
        state.setStatus(newStatus);
        save(state);
        return true;
    }

    /** Парсит {@link MigrationState} из raw JSON записи {@code etcd}. */
    public Optional<MigrationState> readFromEntry(EtcdClientFacade.EtcdEntry entry) {
        try {
            return Optional.of(objectMapper.readValue(entry.json(), MigrationState.class));
        } catch (Exception e) {
            throw new EtcdOperationException("Failed to parse migration state", e);
        }
    }
}
