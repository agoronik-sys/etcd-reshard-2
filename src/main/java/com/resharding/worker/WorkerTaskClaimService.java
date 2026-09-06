package com.resharding.worker;

import com.resharding.config.EtcdProperties;
import com.resharding.config.InstanceProperties;
import com.resharding.domain.MigrationTask;
import com.resharding.domain.TaskStatus;
import com.resharding.etcd.EtcdClientFacade;
import com.resharding.etcd.EtcdKeyPaths;
import com.resharding.etcd.TaskRepository;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.UUID;

/**
 * Захват и ownership Task Worker'ом.
 *
 * <p>Task обнаруживаются <strong>чтением {@code etcd}</strong>, а не через
 * in-memory уведомление: Leader и Worker обычно живут на разных Pod, поэтому
 * локальная очередь до Worker не доходит.
 *
 * <p>Ownership строится на отдельном lock-ключе
 * ({@code {prefix}/migration/{id}/locks/task/{taskId}}) с lease:
 * <ul>
 *   <li>lock захватывается атомарно через {@code put-if-absent};</li>
 *   <li>lease продлевается, пока Task выполняется;</li>
 *   <li>при падении Pod lock исчезает по TTL, а <em>сама Task остаётся</em>
 *       в {@code etcd} — Leader переводит её в {@link TaskStatus#AVAILABLE}
 *       с увеличенным {@code generation}.</li>
 * </ul>
 *
 * <p>Lease намеренно не привязан к ключу Task: иначе падение Worker удалило бы
 * саму Task вместе с её диапазоном.
 */
@Slf4j
@Service
public class WorkerTaskClaimService {

    private final TaskRepository taskRepository;
    private final InstanceProperties instanceProperties;
    private final EtcdClientFacade etcd;
    private final EtcdKeyPaths keys;
    private final EtcdProperties etcdProperties;

    private final ScheduledExecutorService keepAliveScheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "task-lease-keepalive");
                thread.setDaemon(true);
                return thread;
            });

    private volatile String currentTaskId;
    private volatile String currentLockKey;
    private volatile long currentGeneration = -1;
    private volatile long currentTaskRevision = -1;
    private volatile long taskLeaseId = -1;
    private volatile TaskLock currentLock;

    public WorkerTaskClaimService(
            TaskRepository taskRepository,
            InstanceProperties instanceProperties,
            EtcdClientFacade etcd,
            EtcdKeyPaths keys,
            EtcdProperties etcdProperties) {
        this.taskRepository = taskRepository;
        this.instanceProperties = instanceProperties;
        this.etcd = etcd;
        this.keys = keys;
        this.etcdProperties = etcdProperties;

        long intervalSeconds = Math.max(1, etcdProperties.getLeaseTtlSeconds() / 3L);
        keepAliveScheduler.scheduleWithFixedDelay(
                this::keepAliveCurrentTask, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    /** {@code true} если Pod уже выполняет Task. */
    public boolean hasActiveTask() {
        return currentTaskId != null;
    }

    /**
     * Ищет в {@code etcd} свободную Task активной migration и пытается её захватить.
     *
     * @param migrationId идентификатор текущей migration
     */
    public Optional<MigrationTask> tryClaimNext(String migrationId) {
        if (hasActiveTask()) {
            return Optional.empty();
        }

        List<TaskRepository.VersionedTask> claimable = taskRepository.listAllVersioned(migrationId).stream()
                .filter(item -> item.task().getStatus() == TaskStatus.CREATED
                        || item.task().getStatus() == TaskStatus.AVAILABLE)
                .sorted(Comparator.comparingInt(item -> item.task().getSequenceNumber()))
                .toList();

        for (TaskRepository.VersionedTask task : claimable) {
            Optional<MigrationTask> claimed = tryClaim(migrationId, task.task().getTaskId());
            if (claimed.isPresent()) {
                return claimed;
            }
        }
        return Optional.empty();
    }

    /**
     * Захватывает конкретную Task: берёт lock с lease и увеличивает {@code generation}.
     *
     * @return {@link Optional#empty()} если Task уже занята или Pod выполняет другую Task
     */
    public Optional<MigrationTask> tryClaim(String migrationId, String taskId) {
        if (hasActiveTask()) {
            return Optional.empty();
        }

        Optional<TaskRepository.VersionedTask> taskOpt = taskRepository.getVersioned(migrationId, taskId);
        if (taskOpt.isEmpty()) {
            return Optional.empty();
        }

        TaskRepository.VersionedTask versioned = taskOpt.get();
        MigrationTask task = versioned.task();
        if (task.getStatus() != TaskStatus.CREATED && task.getStatus() != TaskStatus.AVAILABLE) {
            return Optional.empty();
        }

        String lockKey = keys.taskLock(migrationId, taskId);
        long leaseId = etcd.grantLease(etcdProperties.getLeaseTtlSeconds());
        long generation = task.getGeneration() + 1;
        String claimToken = UUID.randomUUID().toString();
        TaskLock lock = new TaskLock(
                instanceProperties.getInstanceId(), taskId, generation, claimToken);

        task.setWorkerId(instanceProperties.getInstanceId());
        task.setClaimToken(claimToken);
        task.setGeneration(generation);
        task.setStatus(TaskStatus.ASSIGNED);
        task.setUpdatedAt(LocalDateTime.now());

        /*
         * Критический инвариант: lock и ASSIGNED записываются одной etcd txn.
         * Условия txn проверяют revision прочитанной Task и отсутствие lock.
         * Поэтому ровно один Worker выигрывает claim, а падение Pod не оставляет
         * промежуточного состояния «lock есть, но Task ещё свободна».
         */
        var claimedRevision = taskRepository.claim(
                migrationId, task, versioned.revision(), lockKey, lock, leaseId);
        if (claimedRevision.isEmpty()) {
            etcd.revokeLease(leaseId);
            return Optional.empty();
        }

        currentTaskId = taskId;
        currentLockKey = lockKey;
        currentGeneration = generation;
        currentTaskRevision = claimedRevision.getAsLong();
        taskLeaseId = leaseId;
        currentLock = lock;

        log.info("Claimed task {} generation {}", taskId, generation);
        return Optional.of(task);
    }

    /**
     * Проверяет, что текущий Pod по-прежнему владеет Task (fencing).
     * Старый Worker с устаревшим {@code generation} получит {@code false}.
     */
    public boolean validateOwnership(MigrationTask task) {
        TaskLock lock = currentLock;
        if (lock == null
                || task.getWorkerId() == null
                || !task.getWorkerId().equals(instanceProperties.getInstanceId())
                || task.getGeneration() != currentGeneration
                || !lock.claimToken().equals(task.getClaimToken())) {
            return false;
        }

        Optional<TaskRepository.VersionedTask> persisted =
                taskRepository.getVersioned(task.getMigrationId(), task.getTaskId());
        Optional<TaskLock> persistedLock = etcd.get(currentLockKey, TaskLock.class);
        return persisted.isPresent()
                && persisted.get().revision() == currentTaskRevision
                && persisted.get().task().getGeneration() == currentGeneration
                && lock.claimToken().equals(persisted.get().task().getClaimToken())
                && persistedLock.filter(lock::equals).isPresent();
    }

    /**
     * Сохраняет RUNNING/checkpoint только пока Task revision и claim lock принадлежат
     * текущей попытке. CAS false — fencing loss: вызывающий обязан немедленно остановиться.
     */
    public synchronized boolean updateOwnedTask(MigrationTask task) {
        TaskLock lock = currentLock;
        if (lock == null || currentTaskRevision <= 0) {
            return false;
        }
        var revision = taskRepository.updateOwned(
                task.getMigrationId(), task, currentTaskRevision, currentLockKey, lock);
        if (revision.isEmpty()) {
            return false;
        }
        currentTaskRevision = revision.getAsLong();
        return true;
    }

    /**
     * Завершает Task после успешной обработки всего диапазона.
     *
     * @throws IllegalStateException если ownership потерян (fencing)
     */
    public synchronized void completeTask(String migrationId, MigrationTask task) {
        TaskLock lock = currentLock;
        if (lock == null) {
            throw new IllegalStateException("Cannot complete task: ownership lost (fencing)");
        }
        task.setStatus(TaskStatus.COMPLETED);
        task.setUpdatedAt(LocalDateTime.now());
        var completedRevision = taskRepository.completeOwned(
                migrationId, task, currentTaskRevision, currentLockKey, lock);
        if (completedRevision.isEmpty()) {
            throw new IllegalStateException("Cannot complete task: etcd fencing CAS lost");
        }
        long leaseId = taskLeaseId;
        clearLocalOwnership();
        etcd.revokeLease(leaseId);
    }

    /**
     * Кооперативно возвращает Task в очередь (например, при STOP), сохраняя cursor.
     * Сначала fenced-CAS меняет статус, затем удаляется lease-lock.
     */
    public synchronized boolean releaseForRetry(MigrationTask task) {
        task.setStatus(TaskStatus.AVAILABLE);
        task.setWorkerId(null);
        task.setClaimToken(null);
        task.setUpdatedAt(LocalDateTime.now());
        boolean updated = updateOwnedTask(task);
        releaseCurrentTask();
        return updated;
    }

    /** Освобождает lock и сбрасывает локальное состояние текущей Task. */
    public synchronized void releaseCurrentTask() {
        String lockKey = currentLockKey;
        long leaseId = taskLeaseId;
        clearLocalOwnership();

        if (lockKey != null) {
            try {
                etcd.delete(lockKey);
            } catch (RuntimeException e) {
                log.warn("Failed to delete task lock {}", lockKey, e);
            }
        }
        if (leaseId > 0) {
            etcd.revokeLease(leaseId);
        }
    }

    private void clearLocalOwnership() {
        currentTaskId = null;
        currentLockKey = null;
        currentGeneration = -1;
        currentTaskRevision = -1;
        taskLeaseId = -1;
        currentLock = null;
    }

    /**
     * {@code true} если Task числится за Worker, но её lock истёк (Pod упал).
     * Используется Leader'ом для переназначения Task.
     */
    public boolean isTaskLockExpired(String migrationId, String taskId) {
        return !etcd.exists(keys.taskLock(migrationId, taskId));
    }

    /** Централизованный путь lock нужен Leader'у для conditional CAS stale-release. */
    public String taskLockKey(String migrationId, String taskId) {
        return keys.taskLock(migrationId, taskId);
    }

    @PreDestroy
    public void shutdown() {
        keepAliveScheduler.shutdownNow();
        if (currentTaskId != null) {
            releaseCurrentTask();
        }
    }

    private void keepAliveCurrentTask() {
        long leaseId = taskLeaseId;
        if (leaseId <= 0) {
            return;
        }
        try {
            etcd.keepAliveLease(leaseId);
        } catch (RuntimeException e) {
            log.warn("Failed to keep task lease {} alive for task {}", leaseId, currentTaskId, e);
        }
    }

    /** Точное значение lease-lock; token отличает разные claim одного Worker. */
    public record TaskLock(String workerId, String taskId, long generation, String claimToken) {}
}
