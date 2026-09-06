package com.resharding.leader.slot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.resharding.config.DatabaseLimitsProperties;
import com.resharding.config.ReshardingRootProperties;
import com.resharding.domain.MigrationTask;
import com.resharding.etcd.EtcdClientFacade;
import com.resharding.etcd.EtcdKeyPaths;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Управление слотами параллельного доступа к DB.
 *
 * <p>Ограничивает количество Task, одновременно читающих/пишущих в конкретный shard,
 * согласно конфигурации {@code databaseLimits.*}. Каждый слот — отдельный ключ в {@code etcd}
 * ({@code {etcd.prefix}slots/source/{dbId}/{slotId}}), создаваемый через {@code put-if-absent}.
 *
 * <p>Это предотвращает перегрузку Source/Target DB даже при большом числе Worker Pod.
 */
@Component
public class DatabaseSlotManager {

    private final EtcdClientFacade etcd;
    private final EtcdKeyPaths keys;
    private final DatabaseLimitsProperties databaseLimitsProperties;
    private final ObjectMapper objectMapper;

    public DatabaseSlotManager(
            EtcdClientFacade etcd,
            EtcdKeyPaths keys,
            DatabaseLimitsProperties databaseLimitsProperties,
            ObjectMapper objectMapper) {
        this.etcd = etcd;
        this.keys = keys;
        this.databaseLimitsProperties = databaseLimitsProperties;
        this.objectMapper = objectMapper;
    }

    /**
     * Пытается захватить слоты source (и при необходимости target) для Task.
     *
     * @return {@code false} если лимит DB исчерпан — Task не должна создаваться
     */
    public boolean tryAcquireSlots(MigrationTask task) {
        ReshardingRootProperties.DatabaseLimit limit = databaseLimitsProperties.getLimits()
                .getOrDefault(task.getSourceDb(), defaultLimit());

        Optional<String> sourceSlot = acquireSlot(
                "source",
                task.getSourceDb(),
                limit.getMaxSourceTasks(),
                task.getMigrationId(),
                task.getTaskId());
        if (sourceSlot.isEmpty()) {
            return false;
        }

        return true;
    }

    /** Освобождает слоты, захваченные для Task. */
    public void releaseSlots(MigrationTask task) {
        for (var entry : etcd.getPrefix(keys.sourceSlotsPrefix(task.getSourceDb()))) {
            SlotHolder holder = parse(entry.value());
            if (task.getTaskId().equals(holder.taskId())
                    && task.getMigrationId().equals(holder.migrationId())) {
                etcd.compareAndDelete(entry.key(), entry.revision());
            }
        }
    }

    /**
     * Удаляет orphan slots после смены Leader.
     *
     * <p>Раньше ключи жили в etcd без lease, а сведения о них — только в HashMap
     * старого Leader. Новый Leader не мог освободить такие слоты и планирование
     * останавливалось навсегда. Теперь источником истины являются Task в etcd.
     */
    public void reconcileSlots(String migrationId, List<MigrationTask> tasks) {
        Set<String> activeTaskIds = tasks.stream()
                .filter(task -> task.getStatus() != com.resharding.domain.TaskStatus.COMPLETED)
                .map(MigrationTask::getTaskId)
                .collect(java.util.stream.Collectors.toSet());
        Set<String> dbIds = new HashSet<>(databaseLimitsProperties.getLimits().keySet());
        tasks.stream().map(MigrationTask::getSourceDb).forEach(dbIds::add);

        for (String dbId : dbIds) {
            for (var entry : etcd.getPrefix(keys.sourceSlotsPrefix(dbId))) {
                SlotHolder holder = parse(entry.value());
                if (migrationId.equals(holder.migrationId())
                        && !activeTaskIds.contains(holder.taskId())) {
                    etcd.compareAndDelete(entry.key(), entry.revision());
                }
            }
        }
    }

    private Optional<String> acquireSlot(
            String type, String dbId, int maxSlots, String migrationId, String taskId) {
        for (int i = 1; i <= maxSlots; i++) {
            String slotId = String.format("%03d", i);
            String key = "source".equals(type)
                    ? keys.sourceSlot(dbId, slotId)
                    : keys.targetSlot(dbId, slotId);
            SlotHolder holder = new SlotHolder(migrationId, taskId);
            if (etcd.putIfAbsent(key, holder, null)) {
                return Optional.of(key);
            }
        }
        return Optional.empty();
    }

    private ReshardingRootProperties.DatabaseLimit defaultLimit() {
        ReshardingRootProperties.DatabaseLimit limit = new ReshardingRootProperties.DatabaseLimit();
        limit.setMaxSourceTasks(2);
        limit.setMaxTargetTasks(2);
        return limit;
    }

    private SlotHolder parse(String json) {
        try {
            return objectMapper.readValue(json, SlotHolder.class);
        } catch (Exception e) {
            throw new com.resharding.etcd.EtcdOperationException("Failed to parse DB slot", e);
        }
    }

    private record SlotHolder(String migrationId, String taskId) {}
}
