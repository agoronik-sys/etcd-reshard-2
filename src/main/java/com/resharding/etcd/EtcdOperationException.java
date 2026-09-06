package com.resharding.etcd;

/**
 * Исключение при ошибках операций с {@code etcd}.
 *
 * <p>Оборачивает прерывания, таймауты, ошибки сериализации и сбои транзакций jetcd.
 */
public class EtcdOperationException extends RuntimeException {

    public EtcdOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public EtcdOperationException(String message) {
        super(message);
    }
}
