package com.resharding.worker;

import com.resharding.config.MigrationProperties;
import com.resharding.config.ReshardingRootProperties;
import com.resharding.config.TopologyProperties;
import com.resharding.domain.InsertMode;
import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationTask;
import com.resharding.migration.NewShardResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
/**
 * Проверяет operational limits размера read/write batch.
 *
 * <p>Reclaim должен использовать маленькую пачку, production source — уменьшать
 * нагрузку делителем, а новый target допускает большой COPY batch. Ошибка здесь
 * не меняет маршрутизацию, но способна перегрузить рабочую PostgreSQL/autovacuum.
 */
class BatchSizeResolverTest {

    @Mock
    private TopologyProperties topologyProperties;

    @Mock
    private NewShardResolver newShardResolver;

    private MigrationProperties migrationProperties;
    private BatchSizeResolver resolver;

    @BeforeEach
    void setUp() {
        migrationProperties = new MigrationProperties();
        migrationProperties.setCopyBatchSize(50_000);
        migrationProperties.setIdempotentInsertBatchSize(500);
        migrationProperties.setActiveSourceBatchDivisor(10);
        resolver = new BatchSizeResolver(migrationProperties, topologyProperties, newShardResolver);

        when(topologyProperties.getTopologies()).thenReturn(Map.of(
                "v1", topology("v1", List.of("db1", "db2")),
                "v2", topology("v2", List.of("db1", "db2", "db3", "db4"))));
    }

    @Test
    void idempotentInsertUsesSmallBatch() {
        MigrationTask task = task("db1");
        MigrationState migration = migration("v1");

        BatchSizing sizing = resolver.resolve(task, migration, tableConfig(), true);

        assertThat(sizing.getReadBatchSize()).isEqualTo(500);
        assertThat(sizing.getWriteBatchSize()).isEqualTo(500);
        assertThat(sizing.getInsertMode()).isEqualTo(InsertMode.IDEMPOTENT_INSERT);
    }

    @Test
    void copyFromActiveSourceReducesReadBatch() {
        MigrationTask task = task("db1");
        MigrationState migration = migration("v1");

        BatchSizing sizing = resolver.resolve(task, migration, tableConfig(), false);

        assertThat(sizing.getReadBatchSize()).isEqualTo(5_000);
        assertThat(sizing.getWriteBatchSize()).isEqualTo(50_000);
        assertThat(sizing.isActiveSource()).isTrue();
    }

    @Test
    void copyToNewShardKeepsFullWriteBatch() {
        MigrationTask task = task("db1");
        MigrationState migration = migration("v1");
        when(newShardResolver.isNewShard("db3", "v1", "v2")).thenReturn(true);

        BatchSizing sizing = resolver.resolveForTarget(task, migration, tableConfig(), "db3", false);

        assertThat(sizing.getWriteBatchSize()).isEqualTo(50_000);
        assertThat(sizing.getInsertMode()).isEqualTo(InsertMode.COPY);
    }

    @Test
    void writeToActiveTargetShardIsReduced() {
        MigrationTask task = task("db1");
        MigrationState migration = migration("v1");
        when(newShardResolver.isNewShard("db2", "v1", "v2")).thenReturn(false);

        BatchSizing sizing = resolver.resolveForTarget(task, migration, tableConfig(), "db2", false);

        assertThat(sizing.getWriteBatchSize()).isEqualTo(5_000);
        assertThat(sizing.getInsertMode()).isEqualTo(InsertMode.ROW_INSERT);
    }

    private static MigrationTask task(String sourceDb) {
        return MigrationTask.builder()
                .taskId("task-1")
                .sourceDb(sourceDb)
                .currentTopology("v1")
                .targetTopology("v2")
                .build();
    }

    private static MigrationState migration(String currentTopology) {
        return MigrationState.builder().currentTopology(currentTopology).build();
    }

    private static ReshardingRootProperties.TableConfig tableConfig() {
        return new ReshardingRootProperties.TableConfig();
    }

    private static ReshardingRootProperties.TopologyDefinition topology(String version, List<String> shards) {
        ReshardingRootProperties.TopologyDefinition t = new ReshardingRootProperties.TopologyDefinition();
        t.setVersion(version);
        t.setShards(shards);
        return t;
    }
}
