package com.resharding.domain;

/**
 * Алгоритм определения target DB shard по значению shard key.
 */
public enum ShardAlgorithm {

    /**
     * Ketama consistent hash как в nginx {@code hash ... consistent}.
     * CRC32, 160 points на server; должен совпадать с маршрутизацией Nginx + Ketama.
     */
    KETAMA,

    /** Простой {@code CRC32 % N} (legacy / отладка). */
    HASH_MOD;

    public static ShardAlgorithm from(String value) {
        if (value == null || value.isBlank()) {
            return KETAMA;
        }
        return ShardAlgorithm.valueOf(value.trim().toUpperCase());
    }
}
