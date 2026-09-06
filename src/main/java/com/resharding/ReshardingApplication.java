package com.resharding;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Точка входа распределённого сервиса автоматического решардирования данных между
 * инстансами PostgreSQL.
 *
 * <p>Каждый Pod одновременно участвует в выборах Leader и может выполнять роль Worker.
 * Координация, состояние migration и активные задачи хранятся в {@code etcd}.
 * Подключения к shard-БД управляются через {@link com.resharding.db.DataSourceRegistry}.
 *
 * <p>Автоконфигурация Spring {@code DataSource} отключена намеренно: пулы HikariCP
 * создаются динамически по конфигурации {@code segments.database.*}.
 */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
@EnableScheduling
public class ReshardingApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReshardingApplication.class, args);
    }
}
