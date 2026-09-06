package com.resharding.etcd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.resharding.config.EtcdProperties;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.etcd.jetcd.options.GetOption.SortOrder.ASCEND;
import static io.etcd.jetcd.options.GetOption.SortTarget.KEY;

/**
 * Фасад для работы с {@code etcd} через jetcd.
 *
 * <p>Инкапсулирует JSON-сериализацию, lease, prefix-операции и atomic транзакции
 * (put-if-absent, compare-and-swap). Все публичные методы блокирующие с таймаутом
 * {@value #TXN_TIMEOUT_SECONDS} секунд.
 *
 * <p>CAS используется для:
 * <ul>
 *   <li>блокировки активной migration ({@link #putIfAbsent});</li>
 *   <li>атомарного обновления checkpoint ({@link #compareAndSwap}).</li>
 * </ul>
 */
@Slf4j
@Component
public class EtcdClientFacade {

    private static final long TXN_TIMEOUT_SECONDS = 10;

    private final Client client;
    private final KV kv;
    private final Lease leaseClient;
    private final ObjectMapper objectMapper;

    public EtcdClientFacade(EtcdProperties properties, ObjectMapper objectMapper) {
        this.client = Client.builder()
                .endpoints(properties.getEndpoints().toArray(new String[0]))
                .build();
        this.kv = client.getKVClient();
        this.leaseClient = client.getLeaseClient();
        this.objectMapper = objectMapper;
    }

    /**
     * Читает значение ключа и десериализует в указанный тип.
     *
     * @param key  полный ключ {@code etcd}
     * @param type класс для десериализации JSON
     * @return значение или {@link Optional#empty()}, если ключ не существует
     */
    public <T> Optional<T> get(String key, Class<T> type) {
        try {
            GetResponse response = kv.get(bs(key)).get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (response.getKvs().isEmpty()) {
                return Optional.empty();
            }
            String json = response.getKvs().getFirst().getValue().toString(StandardCharsets.UTF_8);
            return Optional.of(objectMapper.readValue(json, type));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while reading key " + key, e);
        } catch (ExecutionException | TimeoutException | JsonProcessingException e) {
            throw new EtcdOperationException("Failed to read key " + key, e);
        }
    }

    /**
     * Читает значение вместе с {@code modRevision} для последующего CAS.
     *
     * @param key полный ключ {@code etcd}
     * @return JSON и revision или {@link Optional#empty()}
     */
    public Optional<EtcdEntry> getWithRevision(String key) {
        try {
            GetResponse response = kv.get(bs(key)).get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (response.getKvs().isEmpty()) {
                return Optional.empty();
            }
            var kvEntry = response.getKvs().getFirst();
            String json = kvEntry.getValue().toString(StandardCharsets.UTF_8);
            return Optional.of(new EtcdEntry(json, kvEntry.getModRevision()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while reading key " + key, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EtcdOperationException("Failed to read key " + key, e);
        }
    }

    public List<KeyValuePair> getPrefix(String prefix) {
        try {
            GetOption option = GetOption.newBuilder()
                    .isPrefix(true)
                    .withSortField(KEY)
                    .withSortOrder(ASCEND)
                    .build();
            GetResponse response = kv.get(bs(prefix), option).get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return response.getKvs().stream()
                    .map(kv -> new KeyValuePair(
                            kv.getKey().toString(StandardCharsets.UTF_8),
                            kv.getValue().toString(StandardCharsets.UTF_8),
                            kv.getModRevision()))
                    .toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while reading prefix " + prefix, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EtcdOperationException("Failed to read prefix " + prefix, e);
        }
    }

    public void put(String key, Object value) {
        put(key, value, null);
    }

    public void put(String key, Object value, Long leaseId) {
        try {
            String json = objectMapper.writeValueAsString(value);
            PutOption.Builder builder = PutOption.newBuilder();
            if (leaseId != null) {
                builder.withLeaseId(leaseId);
            }
            kv.put(bs(key), bs(json), builder.build())
                    .get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while writing key " + key, e);
        } catch (ExecutionException | TimeoutException | JsonProcessingException e) {
            throw new EtcdOperationException("Failed to write key " + key, e);
        }
    }

    /**
     * Atomic {@code put-if-absent}: записывает ключ только если он ещё не существует.
     *
     * @param key     полный ключ
     * @param value   объект для сериализации в JSON
     * @param leaseId optional lease ID; {@code null} — без lease
     * @return {@code true} если запись создана, {@code false} если ключ уже существует
     */
    public boolean putIfAbsent(String key, Object value, Long leaseId) {
        try {
            String json = objectMapper.writeValueAsString(value);
            PutOption.Builder builder = PutOption.newBuilder();
            if (leaseId != null) {
                builder.withLeaseId(leaseId);
            }
            TxnResponse response = kv.txn()
                    .If(new Cmp(bs(key), Cmp.Op.EQUAL, CmpTarget.version(0)))
                    .Then(Op.put(bs(key), bs(json), builder.build()))
                    .commit()
                    .get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return response.isSucceeded();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted during putIfAbsent " + key, e);
        } catch (ExecutionException | TimeoutException | JsonProcessingException e) {
            throw new EtcdOperationException("Failed putIfAbsent " + key, e);
        }
    }

    /**
     * Compare-and-swap по {@code modRevision}: обновляет ключ только если revision совпадает.
     *
     * @param key               полный ключ
     * @param expectedRevision  ожидаемая revision из {@link #getWithRevision}
     * @param newValue          новое значение
     * @return {@code true} если транзакция успешна
     */
    public boolean compareAndSwap(String key, long expectedRevision, Object newValue) {
        try {
            String json = objectMapper.writeValueAsString(newValue);
            TxnResponse response = kv.txn()
                    .If(new Cmp(bs(key), Cmp.Op.EQUAL, CmpTarget.modRevision(expectedRevision)))
                    .Then(Op.put(bs(key), bs(json), PutOption.DEFAULT))
                    .commit()
                    .get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return response.isSucceeded();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted during CAS " + key, e);
        } catch (ExecutionException | TimeoutException | JsonProcessingException e) {
            throw new EtcdOperationException("Failed CAS " + key, e);
        }
    }

    /**
     * Атомарно захватывает Task: проверяет revision Task и отсутствие lock, затем
     * одной etcd-транзакцией записывает и lease-lock, и новое состояние Task.
     *
     * <p>Раздельные {@code putIfAbsent(lock)} и {@code put(task)} оставляли окно:
     * Pod мог упасть между операциями, а lock существовал без назначенной Task.
     *
     * @return новая {@code modRevision} Task или empty при проигранном CAS
     */
    public OptionalLong claimTask(
            String taskKey,
            long expectedTaskRevision,
            Object newTask,
            String lockKey,
            Object lockValue,
            long leaseId) {
        try {
            String taskJson = objectMapper.writeValueAsString(newTask);
            String lockJson = objectMapper.writeValueAsString(lockValue);
            PutOption lockOption = PutOption.newBuilder().withLeaseId(leaseId).build();
            TxnResponse response = kv.txn()
                    .If(
                            new Cmp(bs(taskKey), Cmp.Op.EQUAL, CmpTarget.modRevision(expectedTaskRevision)),
                            new Cmp(bs(lockKey), Cmp.Op.EQUAL, CmpTarget.version(0)))
                    .Then(
                            Op.put(bs(lockKey), bs(lockJson), lockOption),
                            Op.put(bs(taskKey), bs(taskJson), PutOption.DEFAULT))
                    .commit()
                    .get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return response.isSucceeded()
                    ? OptionalLong.of(response.getHeader().getRevision())
                    : OptionalLong.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while claiming task " + taskKey, e);
        } catch (ExecutionException | TimeoutException | JsonProcessingException e) {
            throw new EtcdOperationException("Failed to claim task " + taskKey, e);
        }
    }

    /**
     * CAS-обновление Task, дополнительно fenced точным значением claim lock.
     * Новый Worker создаёт другой token, поэтому zombie Worker не может записать
     * checkpoint даже если локально всё ещё считает себя владельцем.
     */
    public OptionalLong compareAndSwapWithLock(
            String taskKey,
            long expectedTaskRevision,
            Object newTask,
            String lockKey,
            Object expectedLockValue) {
        try {
            String taskJson = objectMapper.writeValueAsString(newTask);
            String lockJson = objectMapper.writeValueAsString(expectedLockValue);
            TxnResponse response = kv.txn()
                    .If(
                            new Cmp(bs(taskKey), Cmp.Op.EQUAL, CmpTarget.modRevision(expectedTaskRevision)),
                            new Cmp(bs(lockKey), Cmp.Op.EQUAL, CmpTarget.value(bs(lockJson))))
                    .Then(Op.put(bs(taskKey), bs(taskJson), PutOption.DEFAULT))
                    .commit()
                    .get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return response.isSucceeded()
                    ? OptionalLong.of(response.getHeader().getRevision())
                    : OptionalLong.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted during fenced CAS " + taskKey, e);
        } catch (ExecutionException | TimeoutException | JsonProcessingException e) {
            throw new EtcdOperationException("Failed fenced CAS " + taskKey, e);
        }
    }

    /** Атомарно завершает Task и удаляет принадлежащий этой попытке lock. */
    public OptionalLong completeTaskWithLock(
            String taskKey,
            long expectedTaskRevision,
            Object completedTask,
            String lockKey,
            Object expectedLockValue) {
        try {
            String taskJson = objectMapper.writeValueAsString(completedTask);
            String lockJson = objectMapper.writeValueAsString(expectedLockValue);
            TxnResponse response = kv.txn()
                    .If(
                            new Cmp(bs(taskKey), Cmp.Op.EQUAL, CmpTarget.modRevision(expectedTaskRevision)),
                            new Cmp(bs(lockKey), Cmp.Op.EQUAL, CmpTarget.value(bs(lockJson))))
                    .Then(
                            Op.put(bs(taskKey), bs(taskJson), PutOption.DEFAULT),
                            Op.delete(bs(lockKey), DeleteOption.DEFAULT))
                    .commit()
                    .get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return response.isSucceeded()
                    ? OptionalLong.of(response.getHeader().getRevision())
                    : OptionalLong.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while completing task " + taskKey, e);
        } catch (ExecutionException | TimeoutException | JsonProcessingException e) {
            throw new EtcdOperationException("Failed to complete task " + taskKey, e);
        }
    }

    /** CAS обновляет key только если другой lock-key отсутствует. */
    public boolean compareAndSwapIfAbsent(
            String key, long expectedRevision, Object newValue, String absentKey) {
        try {
            String json = objectMapper.writeValueAsString(newValue);
            TxnResponse response = kv.txn()
                    .If(
                            new Cmp(bs(key), Cmp.Op.EQUAL, CmpTarget.modRevision(expectedRevision)),
                            new Cmp(bs(absentKey), Cmp.Op.EQUAL, CmpTarget.version(0)))
                    .Then(Op.put(bs(key), bs(json), PutOption.DEFAULT))
                    .commit()
                    .get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return response.isSucceeded();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted during conditional CAS " + key, e);
        } catch (ExecutionException | TimeoutException | JsonProcessingException e) {
            throw new EtcdOperationException("Failed conditional CAS " + key, e);
        }
    }

    /** Удаляет key только при совпадении прочитанной ранее revision. */
    public boolean compareAndDelete(String key, long expectedRevision) {
        try {
            TxnResponse response = kv.txn()
                    .If(new Cmp(bs(key), Cmp.Op.EQUAL, CmpTarget.modRevision(expectedRevision)))
                    .Then(Op.delete(bs(key), DeleteOption.DEFAULT))
                    .commit()
                    .get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return response.isSucceeded();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted during compare-and-delete " + key, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EtcdOperationException("Failed compare-and-delete " + key, e);
        }
    }

    public void delete(String key) {
        try {
            kv.delete(bs(key)).get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while deleting key " + key, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EtcdOperationException("Failed to delete key " + key, e);
        }
    }

    public void deletePrefix(String prefix) {
        try {
            DeleteOption option = DeleteOption.newBuilder().isPrefix(true).build();
            kv.delete(bs(prefix), option).get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while deleting prefix " + prefix, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EtcdOperationException("Failed to delete prefix " + prefix, e);
        }
    }

    /**
     * Создаёт lease с указанным TTL.
     *
     * @param ttlSeconds время жизни lease в секундах
     * @return ID созданного lease
     */
    public long grantLease(int ttlSeconds) {
        try {
            LeaseGrantResponse response = leaseClient.grant(ttlSeconds)
                    .get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return response.getID();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while granting lease", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EtcdOperationException("Failed to grant lease", e);
        }
    }

    /**
     * Продлевает lease без перезаписи значения ключа.
     *
     * <p>Используется Worker'ом, пока Task выполняется: без продления lease
     * истечёт по TTL и Leader переназначит Task другому Pod.
     */
    public void keepAliveLease(long leaseId) {
        try {
            leaseClient.keepAliveOnce(leaseId).get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while keeping lease alive " + leaseId, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EtcdOperationException("Failed to keep lease alive " + leaseId, e);
        }
    }

    /** Отзывает lease: все привязанные к нему ключи удаляются немедленно. */
    public void revokeLease(long leaseId) {
        try {
            leaseClient.revoke(leaseId).get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while revoking lease " + leaseId, e);
        } catch (ExecutionException | TimeoutException e) {
            log.warn("Failed to revoke lease {}; it will expire by TTL", leaseId, e);
        }
    }

    /** {@code true} если ключ существует. */
    public boolean exists(String key) {
        try {
            GetResponse response = kv.get(bs(key), GetOption.newBuilder().withCountOnly(true).build())
                    .get(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return response.getCount() > 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EtcdOperationException("Interrupted while checking key " + key, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EtcdOperationException("Failed to check key " + key, e);
        }
    }

    public CompletableFuture<PutResponse> keepAliveOnce(long leaseId, String key, Object value) {
        try {
            String json = objectMapper.writeValueAsString(value);
            leaseClient.keepAliveOnce(leaseId);
            return kv.put(bs(key), bs(json), PutOption.newBuilder().withLeaseId(leaseId).build());
        } catch (JsonProcessingException e) {
            throw new EtcdOperationException("Failed to serialize value for keepalive", e);
        }
    }

    public Lease leaseClient() {
        return leaseClient;
    }

    @PreDestroy
    public void close() {
        client.close();
    }

    private static ByteSequence bs(String value) {
        return ByteSequence.from(value, StandardCharsets.UTF_8);
    }

    /** Запись ключа с JSON-значением и revision {@code etcd}. */
    public record EtcdEntry(String json, long revision) {}

    /** Пара ключ/значение/revision при prefix-чтении. */
    public record KeyValuePair(String key, String value, long revision) {}
}
