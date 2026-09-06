package com.resharding.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Checkpoint incremental migration для одной таблицы.
 *
 * <p>Отделён от {@link TableCheckpoint}, чтобы incremental-процесс не сканировал
 * постоянно растущий диапазон {@code WHERE created_at >= T0}.
 *
 * <p>Ключ в {@code etcd}:
 * {@code {etcd.prefix}migration/{migrationId}/incremental/{table}/checkpoint}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IncrementalCheckpoint {

    /** Последняя обработанная временная метка incremental-потока. */
    private LocalDateTime lastCreatedAt;

    /** Последний обработанный PK (tie-breaker). */
    private Long lastId;

    /** Время последнего обновления. */
    private LocalDateTime updatedAt;
}
