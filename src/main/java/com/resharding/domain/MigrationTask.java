package com.resharding.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Активная задача переноса данных в рамках sliding window.
 *
 * <p>Task создаётся динамически Leader'ом и существует в {@code etcd} только
 * пока не завершена. После {@link TaskStatus#COMPLETED} метаданные удаляются;
 * прогресс фиксируется в глобальном {@link TableCheckpoint}.
 *
 * <p>Ownership защищён полями {@link #workerId}, {@link #generation} и lease в {@code etcd}.
 * Старый Worker с устаревшим {@code generation} не может подтвердить выполнение (fencing).
 *
 * @see TaskCheckpoint
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MigrationTask {

    /** Уникальный идентификатор задачи, например {@code task-000123}. */
    private String taskId;

    /** Идентификатор родительской migration. */
    private String migrationId;

    /** Имя таблицы в БД (может отличаться от ключа конфигурации). */
    private String table;

    /** Ключ таблицы в {@code application.yml} (например, {@code orders}). */
    private String tableConfigKey;

    /** Идентификатор source DB shard, из которого читаются данные. */
    private String sourceDb;

    /** Версия целевой topology для расчёта target shard. */
    private String targetTopology;

    /** Версия исходной topology (для определения новых shard и создания индексов). */
    private String currentTopology;

    /** Стратегия разбиения диапазона ({@link RangeType#TIME} или {@link RangeType#PK}). */
    private RangeType rangeType;

    /** Начало временного диапазона (для {@link RangeType#TIME}). */
    private LocalDateTime rangeFrom;

    /** Конец временного диапазона (для {@link RangeType#TIME}). */
    private LocalDateTime rangeTo;

    /** Начало PK-диапазона (для {@link RangeType#PK}). */
    private Long rangeFromId;

    /** Конец PK-диапазона (для {@link RangeType#PK}). */
    private Long rangeToId;

    /** {@code instanceId} Worker, выполняющего задачу; {@code null} если не назначена. */
    private String workerId;

    /**
     * Уникальный токен конкретной попытки claim.
     *
     * <p>{@code workerId} сам по себе недостаточен: после истечения lease тот же Pod
     * теоретически может захватить Task повторно. Токен записывается одновременно
     * в Task и lease-lock одной etcd-транзакцией и отличает старую попытку от новой.
     */
    private String claimToken;

    /**
     * Номер попытки выполнения. Увеличивается Worker'ом при каждом успешном claim.
     * Вместе с {@link #claimToken} и {@code modRevision} используется для fencing.
     */
    private long generation;

    /** Текущее состояние задачи. */
    private TaskStatus status;

    /** Локальный checkpoint прогресса внутри диапазона Task. */
    private TaskCheckpoint checkpoint;

    /** Время назначения / начала выполнения. */
    private LocalDateTime startedAt;

    /** Время последнего обновления. */
    private LocalDateTime updatedAt;

    /**
     * Порядковый номер в sliding window. Checkpoint сдвигается только
     * последовательно: нельзя перескочить через незавершённую Task.
     */
    private int sequenceNumber;

    /**
     * {@code true}, если Task была повторно захвачена после release (Pod упал / stale worker).
     * {@code generation > 1} означает, что часть данных могла быть уже скопирована.
     *
     * <p>{@link JsonIgnore} — производное значение, в {@code etcd} не сохраняется.
     */
    @JsonIgnore
    public boolean isReclaimed() {
        return generation > 1;
    }
}
