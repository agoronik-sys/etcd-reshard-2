package com.resharding.etcd;

import com.resharding.config.EtcdProperties;
import com.resharding.domain.MigrationTask;
import com.resharding.domain.TaskCheckpoint;
import com.resharding.domain.TaskStatus;
import com.resharding.worker.WorkerTaskClaimService;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.LocalDateTime;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Интеграционная проверка fencing на настоящем etcd.
 *
 * <p>Воспроизводит главный аварийный сценарий: два Worker одновременно пытаются
 * взять одну Task, затем победитель теряет lease и становится zombie Worker.
 * Проверяем не детали mock-вызовов, а гарантии etcd transaction: claim выигрывает
 * ровно один Worker, а старая revision/token больше не может записать checkpoint.
 */
@Testcontainers(disabledWithoutDocker = true)
class EtcdTaskFencingIntegrationTest {

    @Container
    static final GenericContainer<?> ETCD =
            new GenericContainer<>(DockerImageName.parse("quay.io/coreos/etcd:v3.5.15"))
                    .withExposedPorts(2379)
                    .withCommand(
                            "/usr/local/bin/etcd",
                            "--name=test",
                            "--data-dir=/tmp/etcd-data",
                            "--listen-client-urls=http://0.0.0.0:2379",
                            "--advertise-client-urls=http://0.0.0.0:2379");

    @Test
    void onlyOneConcurrentClaimWinsAndZombieProgressIsRejected() throws Exception {
        EtcdProperties properties = new EtcdProperties();
        properties.setEndpoints(List.of(
                "http://" + ETCD.getHost() + ":" + ETCD.getMappedPort(2379)));
        properties.setPrefix("it/" + UUID.randomUUID() + "/");

        var mapper = new JacksonConfiguration().objectMapper();
        try (FacadeResource resource = new FacadeResource(new EtcdClientFacade(properties, mapper))) {
            EtcdClientFacade etcd = resource.facade();
            EtcdKeyPaths keys = new EtcdKeyPaths(properties);
            TaskRepository repository = new TaskRepository(etcd, keys, mapper);
            String migrationId = "mig-it";
            String taskId = "task-it";
            repository.save(migrationId, baseTask(migrationId, taskId));
            TaskRepository.VersionedTask initial =
                    repository.getVersioned(migrationId, taskId).orElseThrow();

            long lease1 = etcd.grantLease(15);
            long lease2 = etcd.grantLease(15);
            String lockKey = keys.taskLock(migrationId, taskId);
            MigrationTask attempt1 = claimedTask(migrationId, taskId, "worker-1", "token-1");
            MigrationTask attempt2 = claimedTask(migrationId, taskId, "worker-2", "token-2");
            WorkerTaskClaimService.TaskLock lock1 =
                    new WorkerTaskClaimService.TaskLock("worker-1", taskId, 1, "token-1");
            WorkerTaskClaimService.TaskLock lock2 =
                    new WorkerTaskClaimService.TaskLock("worker-2", taskId, 1, "token-2");

            // Оба claim стартуют с одной прочитанной revision — реальная гонка Pod.
            var executor = Executors.newFixedThreadPool(2);
            List<Callable<OptionalLong>> claims = List.of(
                    () -> repository.claim(
                            migrationId, attempt1, initial.revision(), lockKey, lock1, lease1),
                    () -> repository.claim(
                            migrationId, attempt2, initial.revision(), lockKey, lock2, lease2));
            List<OptionalLong> results;
            try {
                results = executor.invokeAll(claims).stream()
                        .map(future -> {
                            try {
                                return future.get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .toList();
            } finally {
                executor.shutdownNow();
            }

            // Task CAS + lock version=0 в одной txn разрешают только одного владельца.
            assertThat(results.stream().filter(OptionalLong::isPresent)).hasSize(1);
            int winner = results.get(0).isPresent() ? 0 : 1;
            long winnerLease = winner == 0 ? lease1 : lease2;
            long loserLease = winner == 0 ? lease2 : lease1;
            MigrationTask zombie = winner == 0 ? attempt1 : attempt2;
            WorkerTaskClaimService.TaskLock winnerLock = winner == 0 ? lock1 : lock2;
            long claimedRevision = results.get(winner).getAsLong();
            etcd.revokeLease(loserLease);

            // Lease loss removes only lock. Leader CAS-releases the durable Task.
            etcd.revokeLease(winnerLease);
            TaskRepository.VersionedTask persisted =
                    repository.getVersioned(migrationId, taskId).orElseThrow();
            MigrationTask available = persisted.task();
            available.setStatus(TaskStatus.AVAILABLE);
            available.setWorkerId(null);
            available.setClaimToken(null);
            assertThat(repository.releaseStale(
                    migrationId, available, persisted.revision(), lockKey)).isTrue();

            // Old worker still has its local object/revision, but exact lock + revision
            // fencing prevents publication of a stale cursor.
            zombie.setCheckpoint(TaskCheckpoint.builder()
                    .lastCreatedAt(LocalDateTime.now())
                    .lastId(100L)
                    .build());
            assertThat(repository.updateOwned(
                    migrationId, zombie, claimedRevision, lockKey, winnerLock)).isEmpty();
        }
    }

    private MigrationTask baseTask(String migrationId, String taskId) {
        return MigrationTask.builder()
                .migrationId(migrationId)
                .taskId(taskId)
                .status(TaskStatus.CREATED)
                .generation(0)
                .build();
    }

    private MigrationTask claimedTask(
            String migrationId, String taskId, String workerId, String token) {
        return MigrationTask.builder()
                .migrationId(migrationId)
                .taskId(taskId)
                .status(TaskStatus.ASSIGNED)
                .workerId(workerId)
                .claimToken(token)
                .generation(1)
                .build();
    }

    private record FacadeResource(EtcdClientFacade facade) implements AutoCloseable {
        @Override
        public void close() {
            facade.close();
        }
    }
}
