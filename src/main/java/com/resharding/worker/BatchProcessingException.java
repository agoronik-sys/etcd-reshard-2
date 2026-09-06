package com.resharding.worker;

/**
 * Ошибка при batch-обработке данных в Worker.
 *
 * <p>Task остаётся в повторяемом состоянии; Leader может перераспределить её
 * другому Worker после истечения lease.
 */
public class BatchProcessingException extends RuntimeException {

    public BatchProcessingException(String message, Throwable cause) {
        super(message, cause);
    }

    public BatchProcessingException(String message) {
        super(message);
    }
}
