package com.resharding.etcd;

import com.resharding.config.EtcdProperties;
import org.springframework.stereotype.Component;

/**
 * Централизованное построение ключей {@code etcd} для сервиса решардирования.
 *
 * <p>Все ключи имеют общий prefix из {@code etcd.prefix} (по умолчанию {@code segments/}).
 * Использование единого класса исключает расхождение путей между компонентами.
 *
 * @see EtcdProperties#getPrefix()
 */
@Component
public class EtcdKeyPaths {

    private final String prefix;

    public EtcdKeyPaths(EtcdProperties properties) {
        this.prefix = properties.getPrefix().endsWith("/")
                ? properties.getPrefix().substring(0, properties.getPrefix().length() - 1)
                : properties.getPrefix();
    }

    /** Ключ текущего Leader: {@code {prefix}/leader}. */
    public String leader() {
        return prefix + "/leader";
    }

    /** Ключ регистрации Pod: {@code {prefix}/members/{instanceId}}. */
    public String member(String instanceId) {
        return prefix + "/members/" + instanceId;
    }

    /** Prefix всех members для list-операций. */
    public String membersPrefix() {
        return prefix + "/members/";
    }

    /** Ключ текущей migration: {@code {prefix}/migration/current}. */
    public String migrationCurrent() {
        return prefix + "/migration/current";
    }

    /** Ключ блокировки единственной активной migration. */
    public String migrationActiveLock() {
        return prefix + "/migration/active-lock";
    }

    /**
     * Checkpoint bulk migration для пары (таблица, source shard).
     *
     * <p>Данные переносятся со всех shard current topology независимо,
     * поэтому прогресс отслеживается отдельно по каждому source.
     */
    public String checkpoint(String migrationId, String table, String sourceDb) {
        return prefix + "/migration/" + migrationId + "/checkpoints/" + table + "/" + sourceDb;
    }

    /** Prefix всех bulk checkpoint для migration. */
    public String checkpointsPrefix(String migrationId) {
        return prefix + "/migration/" + migrationId + "/checkpoints/";
    }

    /** Checkpoint incremental migration для таблицы. */
    public String incrementalCheckpoint(String migrationId, String table) {
        return prefix + "/migration/" + migrationId + "/incremental/" + table + "/checkpoint";
    }

    /** Статус verify-индексов на новом shard для таблицы. */
    public String tableIndexStatus(String migrationId, String dbId, String table) {
        return prefix + "/migration/" + migrationId + "/indexes/" + dbId + "/" + table;
    }

    /** Статус verify-индексов на партиции таблицы. */
    public String tablePartitionIndexStatus(String migrationId, String dbId, String table, String partitionKey) {
        return prefix + "/migration/" + migrationId + "/indexes/" + dbId + "/" + table + "/" + partitionKey;
    }

    /** Prefix всех статусов индексов migration. */
    public String indexesPrefix(String migrationId) {
        return prefix + "/migration/" + migrationId + "/indexes/";
    }

    /** Ключ активной Task. */
    public String task(String migrationId, String taskId) {
        return prefix + "/migration/" + migrationId + "/tasks/" + taskId;
    }

    /** Prefix всех Task для migration. */
    public String tasksPrefix(String migrationId) {
        return prefix + "/migration/" + migrationId + "/tasks/";
    }

    /** Lock ownership Task конкретным Worker. */
    public String taskLock(String migrationId, String taskId) {
        return prefix + "/migration/" + migrationId + "/locks/task/" + taskId;
    }

    /** Слот ограничения параллельных source-операций на DB. */
    public String sourceSlot(String dbId, String slotId) {
        return prefix + "/slots/source/" + dbId + "/" + slotId;
    }

    /** Слот ограничения параллельных target-операций на DB. */
    public String targetSlot(String dbId, String slotId) {
        return prefix + "/slots/target/" + dbId + "/" + slotId;
    }

    /** Prefix source-слотов для DB. */
    public String sourceSlotsPrefix(String dbId) {
        return prefix + "/slots/source/" + dbId + "/";
    }

    /** Prefix target-слотов для DB. */
    public String targetSlotsPrefix(String dbId) {
        return prefix + "/slots/target/" + dbId + "/";
    }

    /** Глобальная конфигурация migration в {@code etcd}. */
    public String migrationConfig() {
        return prefix + "/config/migration";
    }
}
