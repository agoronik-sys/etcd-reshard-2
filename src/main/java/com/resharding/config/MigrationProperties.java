package com.resharding.config;



import lombok.Data;

import org.springframework.boot.context.properties.ConfigurationProperties;



/**

 * Параметры migration: параллельность, batch size, sliding window.

 *

 * <p>Фактическое число Task: {@code min(availableWorkers, maxConcurrentTasks, databaseLimits)}.

 *

 * <p>Размер batch зависит от режима ({@link com.resharding.worker.BatchSizeResolver}):

 * <ul>

 *   <li>{@link #idempotentInsertBatchSize} — ledger-protected INSERT (reclaim);</li>

 *   <li>{@link #copyBatchSize} — COPY через CopyManager на новые shard;</li>

 *   <li>{@link #activeSourceBatchDivisor} — делитель для shard из current topology

 *       (рабочая БД с индексами, без партиционирования).</li>

 * </ul>

 */

@Data

@ConfigurationProperties(prefix = "migration")

public class MigrationProperties {



    /** Максимальное число одновременно выполняемых Task (не захардкожено по Pod). */

    private int maxConcurrentTasks = 5;



    /** Task на Worker; по ТЗ всегда 1. */

    private int taskPerWorker = 1;



    /** Размер batch для COPY на новые shard (CopyManager). */

    private int copyBatchSize = 50_000;



    /** Размер batch для идемпотентного INSERT (reclaim / checkBeforeInsert). */

    private int idempotentInsertBatchSize = 500;



    /**

     * Делитель batch для shard из current topology (активный source).

     * COPY 50 000 → read/write 5 000 на db1/db2, чтобы снизить autovacuum.

     */

    private int activeSourceBatchDivisor = 10;



    /** Длительность одного временного диапазона Task (минуты). */

    private int rangeDurationMinutes = 60;



    /** Интервал incremental cycle (секунды). */

    private int incrementalCycleSeconds = 10;



    /** Интервал tick Leader/Worker scheduler (миллисекунды). */

    private long schedulerIntervalMs = 2000;

    /**
     * Сколько секунд держать пулы соединений после того, как migration перестала быть
     * {@code RUNNING}. Пулы поднимаются лениво и только на время migration
     * ({@link com.resharding.db.DataSourceLifecycleManager}); grace-период защищает
     * от пересоздания пулов при паузах между Task и кратковременной недоступности etcd.
     */
    private int poolReleaseGraceSeconds = 60;

    /** Служебная PostgreSQL-таблица идемпотентности на каждом target shard. */
    private Ledger ledger = new Ledger();

    @Data
    public static class Ledger {
        private String schema = "public";
        private String tableName = "reshard_migration_ledger";
        private boolean autoCreate = true;
    }

}

