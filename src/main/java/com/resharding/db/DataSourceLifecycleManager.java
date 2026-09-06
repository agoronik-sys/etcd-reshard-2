package com.resharding.db;

import com.resharding.config.MigrationProperties;
import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationStatus;
import com.resharding.etcd.MigrationStateRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Управляет жизненным циклом пулов соединений к shard-БД.
 *
 * <p>Пулы существуют только пока есть активная migration:
 * <ul>
 *   <li>migration {@link MigrationStatus#RUNNING} → {@link DataSourceRegistry#activate()};</li>
 *   <li>migration отсутствует, STOPPED, COMPLETED или FAILED → пулы закрываются
 *       после grace-периода {@code migration.pool-release-grace-seconds}.</li>
 * </ul>
 *
 * <p>Grace-период нужен, чтобы кратковременная недоступность {@code etcd}
 * или пауза между Task не приводила к постоянному пересозданию пулов:
 * поднятие пула — это TCP-подключение и аутентификация к каждому shard.
 *
 * <p>Task, прерванная закрытием пула (например, длинный batch на момент STOP),
 * завершится ошибкой, вернётся в пул задач и будет переназначена — прогресс
 * сохранён в checkpoint Task.
 */
@Slf4j
@Service
public class DataSourceLifecycleManager {

    private final DataSourceRegistry dataSourceRegistry;
    private final MigrationStateRepository migrationStateRepository;
    private final MigrationProperties migrationProperties;

    private final AtomicReference<Instant> idleSince = new AtomicReference<>();

    public DataSourceLifecycleManager(
            DataSourceRegistry dataSourceRegistry,
            MigrationStateRepository migrationStateRepository,
            MigrationProperties migrationProperties) {
        this.dataSourceRegistry = dataSourceRegistry;
        this.migrationStateRepository = migrationStateRepository;
        this.migrationProperties = migrationProperties;
    }

    /**
     * Разрешает создание пулов. Вызывается перед работой с данными теми
     * компонентами, которые уже убедились, что migration активна
     * (Leader/Worker после подтверждения RUNNING и появления рабочей Task).
     */
    public void activateForActiveMigration() {
        idleSince.set(null);
        dataSourceRegistry.activate();
    }

    /** Pin пулов на время фактической DB-операции; должен закрываться в finally. */
    public void beginOperation() {
        dataSourceRegistry.beginOperation();
    }

    /** Снимает pin и выполняет отложенное закрытие после STOP/COMPLETED. */
    public void endOperation() {
        dataSourceRegistry.endOperation();
    }

    /**
     * Периодически сверяет состояние migration с состоянием пулов
     * и освобождает соединения, когда migration неактивна.
     */
    @Scheduled(
            fixedDelayString = "${migration.scheduler-interval-ms:2000}",
            scheduler = "lifecycleTaskScheduler")
    public void sweep() {
        boolean migrationActive;
        try {
            migrationActive = migrationStateRepository.getCurrent()
                    .map(MigrationState::getStatus)
                    .filter(status -> status == MigrationStatus.RUNNING)
                    .isPresent();
        } catch (RuntimeException e) {
            log.debug("Cannot read migration state; keeping pools as is", e);
            return;
        }

        if (migrationActive) {
            activateForActiveMigration();
            return;
        }

        releaseAfterGracePeriod();
    }

    private void releaseAfterGracePeriod() {
        if (!dataSourceRegistry.isActive() && dataSourceRegistry.openPools().isEmpty()) {
            return;
        }

        Instant since = idleSince.updateAndGet(current -> current != null ? current : Instant.now());
        Duration grace = Duration.ofSeconds(migrationProperties.getPoolReleaseGraceSeconds());

        if (Duration.between(since, Instant.now()).compareTo(grace) < 0) {
            return;
        }

        log.info("No active migration for {}s: releasing connection pools {}",
                grace.toSeconds(), dataSourceRegistry.openPools());
        dataSourceRegistry.deactivate();
        idleSince.set(null);
    }

    /** Сбрасывает счётчик простоя (для тестов и явного управления). */
    Optional<Instant> idleSince() {
        return Optional.ofNullable(idleSince.get());
    }
}
