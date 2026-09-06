package com.resharding.leader;

import com.resharding.config.MigrationProperties;
import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Incremental migration: обработка данных, появившихся после {@code T0}.
 *
 * <p>Leader запускает цикл примерно каждые {@code incrementalCycleSeconds}.
 * Incremental-процесс использует отдельный {@link com.resharding.domain.IncrementalCheckpoint},
 * чтобы не сканировать постоянно растущий диапазон {@code WHERE created_at >= T0}.
 *
 * <p><strong>Примечание:</strong> полная реализация incremental pipeline — в следующей фазе.
 */
@Slf4j
@Component
public class IncrementalCycleRunner {

    private final MigrationProperties migrationProperties;
    private final AtomicReference<LocalDateTime> lastRun = new AtomicReference<>();

    public IncrementalCycleRunner(MigrationProperties migrationProperties) {
        this.migrationProperties = migrationProperties;
    }

    /**
     * Запускает incremental cycle, если прошло достаточно времени с предыдущего запуска.
     *
     * @param migration текущая RUNNING migration
     */
    public void runIfDue(MigrationState migration) {
        if (migration.getStatus() != MigrationStatus.RUNNING) {
            return;
        }

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime previous = lastRun.get();
        if (previous != null
                && previous.plusSeconds(migrationProperties.getIncrementalCycleSeconds()).isAfter(now)) {
            return;
        }

        lastRun.set(now);
        log.debug("Incremental cycle for migration {} at T0={}", migration.getMigrationId(), migration.getT0());
    }

    /**
     * {@code true} если incremental-процесс догнал поток новых данных.
     * Используется Leader'ом при проверке условия завершения migration.
     *
     * <p><strong>Заглушка:</strong> пока incremental pipeline не реализован,
     * условие всегда выполнено — migration завершается по готовности bulk
     * ({@link BulkCompletionEvaluator}). После реализации здесь должно
     * сравниваться {@link com.resharding.domain.IncrementalCheckpoint}
     * с текущим временем.
     */
    public boolean isCaughtUp(MigrationState migration) {
        return migration.getT0() != null;
    }
}
