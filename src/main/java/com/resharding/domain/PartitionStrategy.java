package com.resharding.domain;

/**
 * Стратегия партиционирования таблицы по дате.
 */
public enum PartitionStrategy {
    /** Месячные партиции ({@code orders_2024_01}). */
    MONTHLY
}
