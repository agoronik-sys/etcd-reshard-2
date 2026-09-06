package com.resharding.domain;

/**
 * Стратегия разбиения данных на диапазоны внутри Task.
 *
 * <p>Для обеих стратегий используется keyset/cursor pagination; {@code OFFSET} запрещён.
 */
public enum RangeType {

    /**
     * Диапазон по временной метке ({@code created_at}) с tie-breaker по PK.
     * Выборка: {@code WHERE (created_at, id) > (:lastCreatedAt, :lastId) ORDER BY created_at, id}.
     */
    TIME,

    /**
     * Диапазон по монотонно возрастающему первичному ключу.
     * Выборка: {@code WHERE id > :lastId ORDER BY id}.
     */
    PK
}
