package com.resharding.db;

/**
 * Ошибка создания verify-индексов на новом DB shard.
 */
public class IndexCreationException extends RuntimeException {

    public IndexCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
