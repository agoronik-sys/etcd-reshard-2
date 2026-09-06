package com.resharding.domain;

/**
 * Состояние единицы работы ({@link MigrationTask}) в sliding window.
 *
 * <p>Task существует в {@code etcd} только пока активна. После {@link #COMPLETED}
 * метаданные удаляются Leader'ом, а глобальный checkpoint сдвигается последовательно.
 */
public enum TaskStatus {

    /** Task создана Leader'ом, ещё не назначена Worker. */
    CREATED,

    /** Task назначена Worker; ownership зафиксирован через lease и {@code generation}. */
    ASSIGNED,

    /** Worker выполняет batch-обработку диапазона. */
    RUNNING,

    /** Все batch диапазона обработаны; ожидает сдвига checkpoint и удаления из {@code etcd}. */
    COMPLETED,

    /** Task завершилась с ошибкой; может быть перераспределена Leader'ом. */
    FAILED,

    /**
     * Task освобождена после истечения lease Worker или его падения.
     * Доступна для повторного захвата другим Worker с увеличенным {@code generation}.
     */
    AVAILABLE
}
