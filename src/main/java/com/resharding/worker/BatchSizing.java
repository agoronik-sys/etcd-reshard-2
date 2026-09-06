package com.resharding.worker;

import com.resharding.domain.InsertMode;
import lombok.Value;

/**
 * Effective размеры batch для READ/WRITE в рамках Task.
 */
@Value
public class BatchSizing {

    /** Размер keyset-выборки из source ({@code LIMIT}). */
    int readBatchSize;

    /** Размер пачки записи на target (COPY или INSERT). */
    int writeBatchSize;

    /** Стратегия записи на target. */
    InsertMode insertMode;

    /** {@code true}, если source — активный production shard (current topology). */
    boolean activeSource;
}
