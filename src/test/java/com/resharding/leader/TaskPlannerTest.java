package com.resharding.leader;

import com.resharding.config.MigrationProperties;
import com.resharding.config.ReshardingRootProperties;
import com.resharding.config.TableMigrationProperties;
import com.resharding.config.TopologyProperties;
import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationTask;
import com.resharding.domain.TableCheckpoint;
import com.resharding.domain.TaskStatus;
import com.resharding.etcd.CheckpointRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
/**
 * Проверяет sliding-window планирование диапазонов.
 *
 * <p>Тесты защищают от повторного создания начального range после restart Leader,
 * перекрытия in-flight Task и пропуска одного из source shard. Детерминированный
 * taskId нужен для идемпотентного повторного планирования.
 */
class TaskPlannerTest {

    private static final LocalDateTime DATE_FROM = LocalDateTime.of(2024, 1, 1, 0, 0);
    private static final LocalDateTime DATE_TO = LocalDateTime.of(2024, 1, 2, 0, 0);

    @Mock
    private TableMigrationProperties tableMigrationProperties;

    @Mock
    private TopologyProperties topologyProperties;

    @Mock
    private CheckpointRepository checkpointRepository;

    private TaskPlanner planner;

    @BeforeEach
    void setUp() {
        MigrationProperties migrationProperties = new MigrationProperties();
        migrationProperties.setRangeDurationMinutes(60);
        planner = new TaskPlanner(
                migrationProperties, tableMigrationProperties, topologyProperties, checkpointRepository);

        Map<String, ReshardingRootProperties.TableConfig> tables = new LinkedHashMap<>();
        tables.put("orders", tableConfig("orders"));
        when(tableMigrationProperties.getEnabledTables()).thenReturn(tables);

        ReshardingRootProperties.TopologyDefinition v1 = new ReshardingRootProperties.TopologyDefinition();
        v1.setVersion("v1");
        v1.setShards(List.of("db1", "db2"));
        when(topologyProperties.getTopologies()).thenReturn(Map.of("v1", v1));

        when(checkpointRepository.getBulkCheckpoint(anyString(), anyString(), anyString()))
                .thenReturn(Optional.empty());
    }

    @Test
    void firstRangeStartsAtDateFrom() {
        List<MigrationTask> planned = planner.planNextTasks(migration(), 1, List.of());

        assertThat(planned).hasSize(1);
        assertThat(planned.getFirst().getRangeFrom()).isEqualTo(DATE_FROM);
        assertThat(planned.getFirst().getRangeTo()).isEqualTo(DATE_FROM.plusHours(1));
    }

    @Test
    void windowSlidesFromCheckpoint() {
        when(checkpointRepository.getBulkCheckpoint(anyString(), eq("orders"), eq("db1")))
                .thenReturn(Optional.of(TableCheckpoint.builder()
                        .table("orders")
                        .sourceDb("db1")
                        .lastProcessedCreatedAt(DATE_FROM.plusHours(5))
                        .build()));

        List<MigrationTask> planned = planner.planNextTasks(migration(), 1, List.of());

        assertThat(planned.getFirst().getRangeFrom()).isEqualTo(DATE_FROM.plusHours(5));
        assertThat(planned.getFirst().getRangeTo()).isEqualTo(DATE_FROM.plusHours(6));
    }

    @Test
    void doesNotOverlapWithInFlightTask() {
        MigrationTask inFlight = MigrationTask.builder()
                .taskId("task-orders-db1-existing")
                .table("orders")
                .sourceDb("db1")
                .status(TaskStatus.RUNNING)
                .rangeFrom(DATE_FROM)
                .rangeTo(DATE_FROM.plusHours(1))
                .build();

        List<MigrationTask> planned = planner.planNextTasks(migration(), 1, List.of(inFlight));

        assertThat(planned.getFirst().getRangeFrom()).isEqualTo(DATE_FROM.plusHours(1));
    }

    @Test
    void plansForEverySourceShard() {
        List<MigrationTask> planned = planner.planNextTasks(migration(), 2, List.of());

        assertThat(planned).extracting(MigrationTask::getSourceDb).containsExactly("db1", "db2");
    }

    @Test
    void rangesWithinOneCycleDoNotOverlap() {
        List<MigrationTask> planned = planner.planNextTasks(migration(), 4, List.of());

        List<MigrationTask> db1Tasks = planned.stream()
                .filter(task -> "db1".equals(task.getSourceDb()))
                .toList();

        assertThat(db1Tasks).hasSize(1);
    }

    @Test
    void stopsWhenDateToReached() {
        when(checkpointRepository.getBulkCheckpoint(anyString(), anyString(), anyString()))
                .thenReturn(Optional.of(TableCheckpoint.builder()
                        .table("orders")
                        .lastProcessedCreatedAt(DATE_TO)
                        .build()));

        assertThat(planner.planNextTasks(migration(), 4, List.of())).isEmpty();
    }

    @Test
    void lastRangeIsClampedToDateTo() {
        when(checkpointRepository.getBulkCheckpoint(anyString(), anyString(), anyString()))
                .thenReturn(Optional.of(TableCheckpoint.builder()
                        .table("orders")
                        .lastProcessedCreatedAt(DATE_TO.minusMinutes(20))
                        .build()));

        List<MigrationTask> planned = planner.planNextTasks(migration(), 1, List.of());

        assertThat(planned.getFirst().getRangeTo()).isEqualTo(DATE_TO);
    }

    @Test
    void taskIdIsDeterministicForSameRange() {
        String first = planner.planNextTasks(migration(), 1, List.of()).getFirst().getTaskId();
        String second = planner.planNextTasks(migration(), 1, List.of()).getFirst().getTaskId();

        assertThat(first).isEqualTo(second).isEqualTo("task-orders-db1-20240101T000000");
    }

    private static MigrationState migration() {
        return MigrationState.builder()
                .migrationId("mig-1")
                .dateFrom(DATE_FROM)
                .dateTo(DATE_TO)
                .currentTopology("v1")
                .targetTopology("v2")
                .build();
    }

    private static ReshardingRootProperties.TableConfig tableConfig(String name) {
        ReshardingRootProperties.TableConfig config = new ReshardingRootProperties.TableConfig();
        config.setTableName(name);
        return config;
    }
}
