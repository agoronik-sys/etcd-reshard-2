package com.resharding.domain;

/**
 * Команды REST API для управления migration.
 *
 * <p>Команды обрабатываются только текущим Leader'ом
 * ({@link com.resharding.cluster.LeaderElectionService#isLeader()}).
 * Это не длительные состояния — они переводят {@link MigrationState}
 * между значениями {@link MigrationStatus}.
 */
public enum MigrationCommand {

    /** Запустить новую migration; отклоняется, если уже есть {@link MigrationStatus#RUNNING}. */
    START,

    /** Остановить текущую migration; Leader прекращает создание Task и incremental-циклов. */
    STOP,

    /** Возобновить ранее остановленную migration с сохранённого checkpoint. */
    RESUME
}
