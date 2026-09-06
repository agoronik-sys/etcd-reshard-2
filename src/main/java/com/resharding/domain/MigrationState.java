package com.resharding.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Текущее состояние migration, хранимое в {@code etcd} по ключу
 * {@code {etcd.prefix}migration/current} (по умолчанию {@code segments/migration/current}).
 *
 * <p>Поле {@link #t0} фиксирует момент фактического старта и разделяет:
 * <ul>
 *   <li>исторические данные ({@code dateFrom}..{@code dateTo}) — bulk migration;</li>
 *   <li>новые данные после {@code T0} — incremental migration.</li>
 * </ul>
 *
 * @see MigrationStatus
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MigrationState {

    /** Уникальный идентификатор migration, например {@code mig-20260905-001}. */
    private String migrationId;

    /** Текущее состояние жизненного цикла. */
    private MigrationStatus status;

    /** Topology, в которой данные находятся на момент старта migration. */
    private String currentTopology;

    /** Целевая topology, к которой необходимо привести физическое расположение данных. */
    private String targetTopology;

    /** Начало исторического диапазона для bulk migration. */
    private LocalDateTime dateFrom;

    /** Конец исторического диапазона для bulk migration. */
    private LocalDateTime dateTo;

    /** Время фактического старта migration (граница bulk / incremental). */
    private LocalDateTime t0;

    /** Время первого запуска migration. */
    private LocalDateTime startedAt;

    /** Время последнего изменения состояния. */
    private LocalDateTime updatedAt;

    /**
     * Проверять наличие строки на target по shard key перед INSERT
     * (уменьшенный batch transactional ledger INSERT).
     * Включается при старте migration с произвольным окном.
     */
    private boolean checkBeforeInsert;
}
