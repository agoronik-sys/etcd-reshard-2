package com.resharding.etcd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.resharding.domain.ClusterMember;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Репозиторий регистрации Pod в кластере.
 *
 * <p>Каждый Pod регистрируется с lease; при падении ключ автоматически удаляется.
 * Leader использует {@link #countActiveWorkers} для расчёта доступной параллельности.
 */
@Repository
public class MemberRepository {

    private final EtcdClientFacade etcd;
    private final EtcdKeyPaths keys;
    private final ObjectMapper objectMapper;

    public MemberRepository(EtcdClientFacade etcd, EtcdKeyPaths keys, ObjectMapper objectMapper) {
        this.etcd = etcd;
        this.keys = keys;
        this.objectMapper = objectMapper;
    }

    /** Регистрирует или обновляет member с привязкой к lease. */
    public void register(ClusterMember member, long leaseId) {
        etcd.put(keys.member(member.getInstanceId()), member, leaseId);
    }

    /** Возвращает member по {@code instanceId}; {@link Optional#empty()} если lease истёк. */
    public Optional<ClusterMember> get(String instanceId) {
        return etcd.get(keys.member(instanceId), ClusterMember.class);
    }

    /** Возвращает все активные members кластера. */
    public List<ClusterMember> listActive() {
        List<ClusterMember> members = new ArrayList<>();
        for (var entry : etcd.getPrefix(keys.membersPrefix())) {
            try {
                members.add(objectMapper.readValue(entry.value(), ClusterMember.class));
            } catch (Exception e) {
                throw new EtcdOperationException("Failed to parse member " + entry.key(), e);
            }
        }
        return members;
    }

    /**
     * Считает доступных Worker (все members кроме Leader).
     *
     * @param leaderInstanceId {@code instanceId} текущего Leader, исключаемый из подсчёта
     * @return количество Pod, способных выполнять Task
     */
    public int countActiveWorkers(String leaderInstanceId) {
        return (int) listActive().stream()
                .filter(m -> !m.getInstanceId().equals(leaderInstanceId))
                .count();
    }
}
