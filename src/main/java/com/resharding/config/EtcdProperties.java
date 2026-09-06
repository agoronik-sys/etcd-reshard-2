package com.resharding.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Параметры подключения к {@code etcd}.
 */
@Data
@ConfigurationProperties(prefix = "etcd")
public class EtcdProperties {

    /** Endpoints кластера etcd. */
    private List<String> endpoints = List.of("http://localhost:2379");

    /** Prefix всех ключей сервиса в {@code etcd}; задаётся в {@code etcd.prefix}, по умолчанию {@code segments/}. */
    private String prefix = "segments/";

    /** TTL lease для Leader, members и Task (секунды). */
    private int leaseTtlSeconds = 30;

    /** Имя ключа Leader (относительно prefix). */
    private String electionKey = "leader";
}
