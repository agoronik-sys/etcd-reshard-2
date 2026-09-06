package com.resharding.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Конфигурация подключений к DB shard'ам ({@code segments.database.*}).
 */
@Data
@ConfigurationProperties(prefix = "segments")
public class SegmentsProperties {

    private Map<String, DatabaseConnectionProperties> database = new HashMap<>();

    /** JDBC-параметры одного DB shard. */
    @Data
    public static class DatabaseConnectionProperties {
        private String url;
        private String username;
        private String password;
        /**
         * Имя server в Ketama ring — как в nginx upstream ({@code host:port}).
         * Если не задано — используется id shard (db1, db2...).
         */
        private String ketamaServer;
        /** Вес server в Ketama ring (nginx default 1). */
        private int ketamaWeight = 1;
    }
}
