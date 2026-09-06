package com.resharding.cluster;

import com.resharding.config.EtcdProperties;
import com.resharding.config.InstanceProperties;
import com.resharding.domain.ClusterMember;
import com.resharding.etcd.EtcdClientFacade;
import com.resharding.etcd.EtcdKeyPaths;
import com.resharding.etcd.MemberRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Регистрация текущего Pod в {@code etcd} с lease.
 *
 * <p>При старте Pod создаёт lease и записывает {@link ClusterMember}.
 * Keepalive продлевает lease; при падении Pod ключ автоматически удаляется,
 * что позволяет Leader обнаружить недоступных Worker и перераспределить Task.
 */
@Slf4j
@Service
public class MemberRegistryService {

    private final MemberRepository memberRepository;
    private final EtcdClientFacade etcd;
    private final EtcdKeyPaths keys;
    private final EtcdProperties etcdProperties;
    private final InstanceProperties instanceProperties;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "member-registry");
        t.setDaemon(true);
        return t;
    });

    private volatile long memberLeaseId = -1;

    public MemberRegistryService(
            MemberRepository memberRepository,
            EtcdClientFacade etcd,
            EtcdKeyPaths keys,
            EtcdProperties etcdProperties,
            InstanceProperties instanceProperties) {
        this.memberRepository = memberRepository;
        this.etcd = etcd;
        this.keys = keys;
        this.etcdProperties = etcdProperties;
        this.instanceProperties = instanceProperties;
    }

    /** Регистрирует Pod и запускает keepalive. */
    @PostConstruct
    public void register() {
        memberLeaseId = etcd.grantLease(etcdProperties.getLeaseTtlSeconds());
        ClusterMember member = ClusterMember.builder()
                .instanceId(instanceProperties.getInstanceId())
                .podName(instanceProperties.getPodName())
                .status("ACTIVE")
                .startedAt(LocalDateTime.now())
                .version(instanceProperties.getVersion())
                .build();
        memberRepository.register(member, memberLeaseId);
        log.info("Registered cluster member: {}", member.getInstanceId());

        scheduler.scheduleWithFixedDelay(
                this::keepAlive,
                etcdProperties.getLeaseTtlSeconds() / 3L,
                etcdProperties.getLeaseTtlSeconds() / 3L,
                TimeUnit.SECONDS);
    }

    @PreDestroy
    public void deregister() {
        scheduler.shutdownNow();
    }

    /**
     * Возвращает количество Pod, доступных как Worker (все members минус Leader).
     *
     * @param leaderInstanceId {@code instanceId} Leader, исключаемый из подсчёта
     */
    public int availableWorkers(String leaderInstanceId) {
        return memberRepository.countActiveWorkers(leaderInstanceId);
    }

    private void keepAlive() {
        ClusterMember member = ClusterMember.builder()
                .instanceId(instanceProperties.getInstanceId())
                .podName(instanceProperties.getPodName())
                .status("ACTIVE")
                .startedAt(LocalDateTime.now())
                .version(instanceProperties.getVersion())
                .build();
        memberRepository.register(member, memberLeaseId);
        etcd.keepAliveOnce(memberLeaseId, keys.member(member.getInstanceId()), member);
    }
}
