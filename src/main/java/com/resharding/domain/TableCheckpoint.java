package com.resharding.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Глобальный checkpoint bulk migration для одной таблицы.
 *
 * <p>Хранится в {@code etcd} по ключу
 * {@code {etcd.prefix}migration/{migrationId}/checkpoints/{table}/{sourceDb}}.
 * Изменяется <strong>только Leader'ом</strong> через atomic CAS-транзакцию.
 *
 * <p>Checkpoint указывает на последний <em>непрерывно завершённый</em> диапазон:
 * нельзя сдвинуть его через незавершённую Task с меньшим {@code sequenceNumber}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableCheckpoint {

    /** Имя таблицы, к которой относится checkpoint. */
    private String table;

    /** Source shard, прогресс которого отслеживает checkpoint. */
    private String sourceDb;

    /** Последняя обработанная временная метка bulk migration. */
    private LocalDateTime lastProcessedCreatedAt;

    /** Последний обработанный PK (tie-breaker при одинаковых timestamp). */
    private Long lastProcessedId;

    /** Время последнего обновления checkpoint. */
    private LocalDateTime updatedAt;

    /**
     * Revision ключа в {@code etcd}; используется для compare-and-swap
     * и предотвращения конкурентной потери обновлений.
     */
    private Long revision;
}
