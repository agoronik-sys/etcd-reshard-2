package com.resharding.worker;

import com.resharding.domain.TaskCheckpoint;
import lombok.Value;

/**
 * Результат обработки одного batch внутри Task.
 *
 * <p>{@link #rowsRead} нужен Worker'у, чтобы понять, исчерпан ли диапазон:
 * если прочитано меньше строк, чем размер batch, — диапазон закончился.
 */
@Value
public class BatchResult {

    /** Позиция курсора после обработки batch. */
    TaskCheckpoint checkpoint;

    /** Сколько строк прочитано из source. */
    int rowsRead;

    /** Сколько строк фактически вставлено на target shard. */
    int rowsInserted;

    /** {@code true}, если диапазон Task полностью обработан. */
    public boolean rangeExhausted(int readBatchSize) {
        return rowsRead < readBatchSize;
    }
}
