package com.resharding.cluster;

import com.resharding.config.EtcdProperties;
import com.resharding.config.InstanceProperties;
import com.resharding.domain.LeaderInfo;
import com.resharding.etcd.EtcdClientFacade;
import com.resharding.etcd.EtcdKeyPaths;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Сервис выборов Leader через {@code etcd}.
 *
 * <p>Каждый Pod периодически участвует в кампании: пытается захватить ключ
 * {@link com.resharding.etcd.EtcdKeyPaths#leader()} через {@code put-if-absent}
 * с lease. Текущий Leader продлевает lease через keepalive.
 *
 * <p>После истечения lease старый Leader теряет право:
 * <ul>
 *   <li>создавать Task;</li>
 *   <li>двигать checkpoint;</li>
 *   <li>менять статус migration;</li>
 *   <li>запускать incremental cycle.</li>
 * </ul>
 *
 * @see com.resharding.leader.LeaderScheduler
 */
@Slf4j
@Service
public class LeaderElectionService {

    private final EtcdClientFacade etcd;
    private final EtcdKeyPaths keys;
    private final EtcdProperties etcdProperties;
    private final InstanceProperties instanceProperties;

    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final AtomicLong currentTerm = new AtomicLong(0);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "leader-election");
        t.setDaemon(true);
        return t;
    });

    private volatile long leaderLeaseId = -1;

    @Getter
    private volatile LeaderInfo leaderInfo;

    public LeaderElectionService(
            EtcdClientFacade etcd,
            EtcdKeyPaths keys,
            EtcdProperties etcdProperties,
            InstanceProperties instanceProperties) {
        this.etcd = etcd;
        this.keys = keys;
        this.etcdProperties = etcdProperties;
        this.instanceProperties = instanceProperties;
    }

    /** Запускает периодическую кампанию за лидерство. */
    @PostConstruct
    public void start() {
        scheduler.scheduleWithFixedDelay(this::campaign, 0, etcdProperties.getLeaseTtlSeconds() / 3L, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop() {
        scheduler.shutdownNow();
        if (isLeader.get()) {
            resign();
        }
    }

    /** {@code true} если текущий Pod является Leader. */
    public boolean isLeader() {
        return isLeader.get();
    }

    /** {@code instanceId} текущего Pod. */
    public String instanceId() {
        return instanceProperties.getInstanceId();
    }

    /**
     * Lease текущего лидерского term. Связанные координационные ключи
     * (в частности active-migration lock) должны жить не дольше Leader.
     */
    public long leaderLeaseId() {
        long leaseId = leaderLeaseId;
        if (!isLeader.get() || leaseId <= 0) {
            throw new IllegalStateException("Current instance does not hold a leader lease");
        }
        return leaseId;
    }

    private void campaign() {
        try {
            if (isLeader.get()) {
                renewLeadership();
            } else {
                tryBecomeLeader();
            }
        } catch (Exception e) {
            log.warn("Leader election cycle failed: {}", e.getMessage());
            isLeader.set(false);
        }
    }

    private void tryBecomeLeader() {
        long leaseId = etcd.grantLease(etcdProperties.getLeaseTtlSeconds());
        long term = currentTerm.incrementAndGet();
        LeaderInfo candidate = LeaderInfo.builder()
                .instanceId(instanceProperties.getInstanceId())
                .term(term)
                .electedAt(LocalDateTime.now())
                .build();

        if (etcd.putIfAbsent(keys.leader(), candidate, leaseId)) {
            leaderLeaseId = leaseId;
            leaderInfo = candidate;
            isLeader.set(true);
            log.info("Elected as leader, term={}, instanceId={}", term, instanceProperties.getInstanceId());
        }
    }

    private void renewLeadership() {
        Optional<LeaderInfo> current = etcd.get(keys.leader(), LeaderInfo.class);
        if (current.isEmpty() || !instanceProperties.getInstanceId().equals(current.get().getInstanceId())) {
            isLeader.set(false);
            log.info("Lost leadership");
            return;
        }

        LeaderInfo renewed = LeaderInfo.builder()
                .instanceId(instanceProperties.getInstanceId())
                .term(current.get().getTerm())
                .electedAt(current.get().getElectedAt())
                .build();

        etcd.keepAliveOnce(leaderLeaseId, keys.leader(), renewed);
        leaderInfo = renewed;
    }

    private void resign() {
        etcd.delete(keys.leader());
        isLeader.set(false);
        leaderLeaseId = -1;
    }
}
