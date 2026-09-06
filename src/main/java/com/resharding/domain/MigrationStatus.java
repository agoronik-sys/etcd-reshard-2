package com.resharding.domain;

/**
 * Состояние жизненного цикла migration.
 *
 * <p>Команды {@link MigrationCommand} (START, STOP, RESUME) переводят migration
 * между этими состояниями. В системе одновременно допускается только одна migration
 * в состоянии {@link #RUNNING}.
 */
public enum MigrationStatus {

    /** Migration не запущена; начальное состояние или после явного сброса. */
    IDLE,

    /** Migration активна: Leader создаёт Task, Worker выполняют перенос данных. */
    RUNNING,

    /** Migration остановлена по команде STOP; прогресс сохранён в {@code etcd}. */
    STOPPED,

    /** Все исторические диапазоны и incremental-процесс завершены успешно. */
    COMPLETED,

    /** Migration завершилась с неустранимой ошибкой. */
    FAILED
}
