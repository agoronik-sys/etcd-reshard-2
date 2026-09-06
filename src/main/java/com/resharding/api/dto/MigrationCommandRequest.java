package com.resharding.api.dto;

import com.resharding.domain.MigrationCommand;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Тело REST-запроса для команды migration.
 *
 * <p>Пример запуска:
 * <pre>{@code
 * {
 *   "dateFrom": "2023-01-01T00:00:00",
 *   "dateTo": "2026-09-01T00:00:00",
 *   "checkBeforeInsert": true,
 *   "status": "START"
 * }
 * }</pre>
 */
@Data
public class MigrationCommandRequest {

    /** Начало исторического диапазона (обязательно для START). */
    private LocalDateTime dateFrom;

    /** Конец исторического диапазона (обязательно для START). */
    private LocalDateTime dateTo;

    /** Команда: {@link MigrationCommand#START}, {@link MigrationCommand#STOP} или {@link MigrationCommand#RESUME}. */
    @NotNull
    private MigrationCommand status;

    /**
     * Использовать уменьшенный batch идемпотентного ledger INSERT с первого прохода.
     * Рекомендуется при старте с произвольным окном.
     */
    private Boolean checkBeforeInsert;
}
