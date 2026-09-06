package com.resharding.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Локальный checkpoint прогресса внутри {@link MigrationTask}.
 *
 * <p>Обновляется Worker'ом после каждого успешно обработанного batch
 * (READ → WRITE → VERIFY → DELETE source). Глобальный {@link TableCheckpoint}
 * изменяет только Leader после полного завершения Task.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskCheckpoint {

    /** Последняя обработанная временная метка (tie-breaker для keyset pagination). */
    private LocalDateTime lastCreatedAt;

    /** Последний обработанный PK (используется совместно с {@link #lastCreatedAt}). */
    private Long lastId;

    /** Последний обработанный PK при стратегии {@link RangeType#PK}. */
    private Long lastProcessedId;
}
