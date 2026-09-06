package com.resharding.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Идентификация текущего Pod.
 */
@Data
@ConfigurationProperties(prefix = "resharding")
public class InstanceProperties {

    /** Уникальный ID экземпляра; по умолчанию из {@code HOSTNAME} или random UUID. */
    private String instanceId;

    /** Имя Pod в Kubernetes. */
    private String podName;

    /** Версия сервиса для регистрации в {@code etcd}. */
    private String version = "1.0.0";
}
