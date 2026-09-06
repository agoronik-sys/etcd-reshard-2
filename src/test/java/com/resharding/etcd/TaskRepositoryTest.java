package com.resharding.etcd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.resharding.config.EtcdProperties;
import com.resharding.domain.MigrationTask;
import com.resharding.worker.WorkerTaskClaimService;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Проверяет, что repository использует атомарные примитивы facade, а не
 * небезопасную последовательность get + put.
 *
 * <p>Это контрактный unit-уровень; конкурентное поведение настоящего etcd
 * дополнительно проверяется в {@link EtcdTaskFencingIntegrationTest}.
 */
class TaskRepositoryTest {

    private final EtcdClientFacade etcd = mock(EtcdClientFacade.class);
    private final EtcdKeyPaths keys = new EtcdKeyPaths(new EtcdProperties());
    private final TaskRepository repository = new TaskRepository(etcd, keys, new ObjectMapper());

    @Test
    void claimDelegatesSingleAtomicTaskAndLockTransaction() {
        MigrationTask task = MigrationTask.builder()
                .migrationId("mig-1")
                .taskId("task-1")
                .build();
        WorkerTaskClaimService.TaskLock lock =
                new WorkerTaskClaimService.TaskLock("worker-1", "task-1", 1, "token");
        when(etcd.claimTask(
                keys.task("mig-1", "task-1"), 10, task,
                keys.taskLock("mig-1", "task-1"), lock, 99))
                .thenReturn(OptionalLong.of(11));

        OptionalLong revision = repository.claim(
                "mig-1", task, 10, keys.taskLock("mig-1", "task-1"), lock, 99);

        assertThat(revision).hasValue(11);
        verify(etcd).claimTask(
                keys.task("mig-1", "task-1"), 10, task,
                keys.taskLock("mig-1", "task-1"), lock, 99);
    }

    @Test
    void staleReleaseRequiresTaskRevisionAndAbsentLock() {
        MigrationTask task = MigrationTask.builder()
                .migrationId("mig-1")
                .taskId("task-1")
                .build();
        String lockKey = keys.taskLock("mig-1", "task-1");
        when(etcd.compareAndSwapIfAbsent(
                keys.task("mig-1", "task-1"), 15, task, lockKey))
                .thenReturn(true);

        assertThat(repository.releaseStale("mig-1", task, 15, lockKey)).isTrue();
        verify(etcd).compareAndSwapIfAbsent(
                keys.task("mig-1", "task-1"), 15, task, lockKey);
    }
}
