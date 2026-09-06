package com.resharding.db;

/**
 * Попытка получить соединение к shard-БД, когда активной migration нет.
 *
 * <p>Пулы поднимаются только на время migration, поэтому обращение к
 * {@link DataSourceRegistry#get(String)} вне migration — признак ошибки
 * в порядке вызовов, а не проблемы с БД.
 */
public class DataSourceInactiveException extends IllegalStateException {

    public DataSourceInactiveException(String message) {
        super(message);
    }
}
