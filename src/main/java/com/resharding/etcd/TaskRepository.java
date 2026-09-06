package com.resharding.etcd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.resharding.domain.MigrationTask;
import com.resharding.domain.TaskStatus;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Репозиторий активных Task migration в {@code etcd}.
 *
 * <p>Task хранятся только пока не завершены; после {@link TaskStatus#COMPLETED}
 * Leader удаляет их, чтобы {@code etcd} не разрастался.
 */
@Repository
public class TaskRepository {

    private final EtcdClientFacade etcd;
    private final EtcdKeyPaths keys;
    private final ObjectMapper objectMapper;

    public TaskRepository(EtcdClientFacade etcd, EtcdKeyPaths keys, ObjectMapper objectMapper) {
        this.etcd = etcd;
        this.keys = keys;
        this.objectMapper = objectMapper;
    }

    /** Сохраняет Task без lease. */
    public void save(String migrationId, MigrationTask task) {
        etcd.put(keys.task(migrationId, task.getTaskId()), task);
    }

    /** Сохраняет Task с lease Worker для автоматического release при падении Pod. */
    public void saveWithLease(String migrationId, MigrationTask task, long leaseId) {
        etcd.put(keys.task(migrationId, task.getTaskId()), task, leaseId);
    }

    /** Возвращает Task по идентификатору. */
    public Optional<MigrationTask> get(String migrationId, String taskId) {
        return etcd.get(keys.task(migrationId, taskId), MigrationTask.class);
    }

    /** Читает Task вместе с etcd modRevision — fencing основан именно на этой revision. */
    public Optional<VersionedTask> getVersioned(String migrationId, String taskId) {
        return etcd.getWithRevision(keys.task(migrationId, taskId))
                .map(entry -> new VersionedTask(parse(entry.json(), entry.revision()), entry.revision()));
    }

    /**
     * Возвращает все Task migration, присутствующие в {@code etcd},
     * включая {@link TaskStatus#COMPLETED} (ещё не удалённые Leader'ом).
     *
     * <p>Leader'у нужны и завершённые Task: по ним двигается checkpoint
     * и освобождаются слоты DB.
     */
    public List<MigrationTask> listAll(String migrationId) {
        return listAllVersioned(migrationId).stream().map(VersionedTask::task).toList();
    }

    /** Возвращает все Task вместе с revision для Leader CAS-операций. */
    public List<VersionedTask> listAllVersioned(String migrationId) {
        List<VersionedTask> tasks = new ArrayList<>();
        for (var entry : etcd.getPrefix(keys.tasksPrefix(migrationId))) {
            tasks.add(new VersionedTask(parse(entry.value(), entry.revision()), entry.revision()));
        }
        return tasks;
    }

    /** Возвращает только незавершённые Task для migration. */
    public List<MigrationTask> listActive(String migrationId) {
        return listAll(migrationId).stream()
                .filter(task -> task.getStatus() != TaskStatus.COMPLETED)
                .toList();
    }

    /** Удаляет метаданные Task из {@code etcd}. */
    public void delete(String migrationId, String taskId) {
        etcd.delete(keys.task(migrationId, taskId));
    }

    /** Атомарный claim: Task CAS и создание lease-lock выполняются одной транзакцией. */
    public OptionalLong claim(
            String migrationId,
            MigrationTask task,
            long expectedRevision,
            String lockKey,
            Object lockValue,
            long leaseId) {
        return etcd.claimTask(
                keys.task(migrationId, task.getTaskId()),
                expectedRevision,
                task,
                lockKey,
                lockValue,
                leaseId);
    }

    /** Fenced progress update: сравниваются revision Task и точное значение lock. */
    public OptionalLong updateOwned(
            String migrationId,
            MigrationTask task,
            long expectedRevision,
            String lockKey,
            Object lockValue) {
        return etcd.compareAndSwapWithLock(
                keys.task(migrationId, task.getTaskId()),
                expectedRevision,
                task,
                lockKey,
                lockValue);
    }

    /** Fenced completion одновременно сохраняет COMPLETED и удаляет claim lock. */
    public OptionalLong completeOwned(
            String migrationId,
            MigrationTask task,
            long expectedRevision,
            String lockKey,
            Object lockValue) {
        return etcd.completeTaskWithLock(
                keys.task(migrationId, task.getTaskId()),
                expectedRevision,
                task,
                lockKey,
                lockValue);
    }

    /** Leader возвращает stale Task только если lock действительно отсутствует. */
    public boolean releaseStale(
            String migrationId, MigrationTask task, long expectedRevision, String lockKey) {
        return etcd.compareAndSwapIfAbsent(
                keys.task(migrationId, task.getTaskId()),
                expectedRevision,
                task,
                lockKey);
    }

    /** Удаляет неизменившуюся Task; проигранный CAS означает retry на следующем tick. */
    public boolean deleteIfRevision(String migrationId, String taskId, long expectedRevision) {
        return etcd.compareAndDelete(keys.task(migrationId, taskId), expectedRevision);
    }

    private MigrationTask parse(String json, long revision) {
        try {
            return objectMapper.readValue(json, MigrationTask.class);
        } catch (Exception e) {
            throw new EtcdOperationException("Failed to parse task at revision " + revision, e);
        }
    }

    public record VersionedTask(MigrationTask task, long revision) {}
}
