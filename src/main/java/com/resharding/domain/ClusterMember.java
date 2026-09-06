package com.resharding.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Регистрация Pod в кластере решардирования.
 *
 * <p>Записывается в {@code etcd} с lease; при падении Pod ключ автоматически
 * удаляется. Leader использует список активных members для расчёта доступных Worker.
 *
 * <p>Ключ: {@code {etcd.prefix}members/{instanceId}}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClusterMember {

    /** Уникальный идентификатор экземпляра сервиса (Pod). */
    private String instanceId;

    /** Имя Pod в Kubernetes или локальный идентификатор. */
    private String podName;

    /** Статус участника кластера ({@code ACTIVE} при нормальной работе). */
    private String status;

    /** Время регистрации экземпляра. */
    private LocalDateTime startedAt;

    /** Версия сервиса. */
    private String version;
}
