package com.resharding.domain;

/**
 * Стратегия записи на target shard.
 */
public enum InsertMode {

    /** Массовая загрузка через PostgreSQL {@code CopyManager} (новые shard). */
    COPY,

    /** Построчный INSERT, защищённый transactional migration-ledger. */
    IDEMPOTENT_INSERT,

    /** Построчный INSERT на активный production shard (осторожный режим). */
    ROW_INSERT
}
