package com.resharding.db;

import com.resharding.config.SegmentsProperties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Реестр пулов HikariCP для DB shard'ов с <strong>ленивой</strong> инициализацией.
 *
 * <p>Пул для shard создаётся при первом обращении и <em>только</em> когда реестр
 * активирован — то есть когда есть активная migration и Pod действительно
 * работает с данными. Пока migration не запущена, Pod не держит ни одного
 * соединения к shard-БД: сервис может быть развёрнут на десятках Pod,
 * и постоянные пулы к каждому shard исчерпали бы {@code max_connections}.
 *
 * <p>Жизненный цикл ведёт {@link DataSourceLifecycleManager}:
 * <pre>
 * migration RUNNING            → activate()   → пулы поднимаются по требованию
 * migration завершена/STOPPED  → deactivate() → пулы закрываются
 * </pre>
 *
 * <p>Параметры пула фиксированы согласно ТЗ:
 * {@code minimumIdle=1}, {@code maximumPoolSize=10}.
 */
@Slf4j
@Component
public class DataSourceRegistry {

    /** Минимальное число idle-соединений (требование ТЗ). */
    private static final int MINIMUM_IDLE = 1;

    /** Максимальный размер пула (требование ТЗ). */
    private static final int MAXIMUM_POOL_SIZE = 10;

    private final SegmentsProperties segmentsProperties;
    private final Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();
    private final AtomicBoolean active = new AtomicBoolean(false);
    private int activeOperations;
    private boolean closeRequested;

    public DataSourceRegistry(SegmentsProperties segmentsProperties) {
        this.segmentsProperties = segmentsProperties;
    }

    /**
     * Разрешает создание пулов. Вызывается при активной migration.
     * Идемпотентно.
     */
    public synchronized void activate() {
        closeRequested = false;
        if (active.compareAndSet(false, true)) {
            log.info("DataSource registry activated: pools will be created on demand");
        }
    }

    /**
     * Запрещает создание новых пулов и закрывает уже открытые.
     * Идемпотентно; вызывается когда активной migration нет.
     */
    public synchronized void deactivate() {
        /*
         * Не закрываем HikariDataSource посередине JDBC batch. STOP запрещает
         * новые Task, а уже начатая операция удерживает pin до безопасной точки.
         */
        if (activeOperations > 0) {
            closeRequested = true;
            return;
        }
        boolean wasActive = active.getAndSet(false);
        if (!wasActive && dataSources.isEmpty()) {
            return;
        }
        closeOpenPools();
    }

    /** {@code true} если создание пулов разрешено. */
    public boolean isActive() {
        return active.get();
    }

    /** Идентификаторы shard, для которых пул уже поднят. */
    public Set<String> openPools() {
        return Set.copyOf(dataSources.keySet());
    }

    /**
     * Возвращает DataSource для shard, создавая пул при первом обращении.
     *
     * @param dbId идентификатор DB ({@code db1}, {@code db2}, ...)
     * @throws IllegalArgumentException      если shard не найден в конфигурации
     * @throws DataSourceInactiveException   если активной migration нет
     */
    public synchronized DataSource get(String dbId) {
        SegmentsProperties.DatabaseConnectionProperties config = segmentsProperties.getDatabase().get(dbId);
        if (config == null) {
            throw new IllegalArgumentException("Unknown database: " + dbId);
        }

        HikariDataSource existing = dataSources.get(dbId);
        if (existing != null) {
            return existing;
        }

        if (!active.get()) {
            throw new DataSourceInactiveException(
                    "Refusing to open connection pool for %s: no active migration".formatted(dbId));
        }

        return dataSources.computeIfAbsent(dbId, id -> createPool(id, config));
    }

    /** Удерживает пулы открытыми на время одной Worker/Leader DB-операции. */
    public synchronized void beginOperation() {
        if (!active.get() || closeRequested) {
            throw new DataSourceInactiveException("Cannot begin DB operation: registry is inactive");
        }
        activeOperations++;
    }

    /** Снимает pin; отложенный STOP закрывает пулы сразу после последней операции. */
    public synchronized void endOperation() {
        if (activeOperations <= 0) {
            throw new IllegalStateException("Unbalanced DataSource operation pin");
        }
        activeOperations--;
        if (activeOperations == 0 && closeRequested) {
            closeRequested = false;
            active.set(false);
            closeOpenPools();
        }
    }

    private HikariDataSource createPool(String dbId, SegmentsProperties.DatabaseConnectionProperties config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getUrl());
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());
        hikariConfig.setPoolName("hikari-" + dbId);
        hikariConfig.setMinimumIdle(MINIMUM_IDLE);
        hikariConfig.setMaximumPoolSize(MAXIMUM_POOL_SIZE);

        log.info("Opening connection pool for shard {} ({})", dbId, config.getUrl());
        return new HikariDataSource(hikariConfig);
    }

    private void closeOpenPools() {
        for (String dbId : Set.copyOf(dataSources.keySet())) {
            HikariDataSource dataSource = dataSources.remove(dbId);
            if (dataSource == null) {
                continue;
            }
            try {
                dataSource.close();
                log.info("Closed connection pool for shard {}", dbId);
            } catch (RuntimeException e) {
                log.warn("Failed to close connection pool for shard {}", dbId, e);
            }
        }
    }

    @PreDestroy
    public synchronized void close() {
        active.set(false);
        closeRequested = false;
        activeOperations = 0;
        closeOpenPools();
    }
}
