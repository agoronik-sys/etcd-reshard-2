package com.resharding.db;

/**
 * Ошибка вставки строки на target shard.
 */
public class TargetRowInsertException extends RuntimeException {

    public TargetRowInsertException(String message, Throwable cause) {
        super(message, cause);
    }
}
