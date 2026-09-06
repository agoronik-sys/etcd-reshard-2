package com.resharding.leader;

import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationTask;
import com.resharding.domain.TableCheckpoint;
import com.resharding.domain.TaskStatus;
import com.resharding.etcd.CheckpointRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
/**
 * Проверяет непрерывное продвижение глобального checkpoint.
 *
 * <p>Leader не имеет права перепрыгнуть незавершённый диапазон, даже если более
 * поздняя Task уже COMPLETED. Checkpoint каждой пары table × sourceDb независим,
 * а CAS conflict должен оставить данные для повторного scheduler tick.
 */
class CheckpointAdvancerTest {

    private static final LocalDateTime DATE_FROM = LocalDateTime.of(2024, 1, 1, 0, 0);

    @Mock
    private CheckpointRepository checkpointRepository;

    @Mock
    private PartitionIndexCoordinator partitionIndexCoordinator;

    private CheckpointAdvancer advancer;

    @BeforeEach
    void setUp() {
        advancer = new CheckpointAdvancer(checkpointRepository, partitionIndexCoordinator);
        when(checkpointRepository.initBulkCheckpoint(anyString(), any())).thenReturn(true);
        when(checkpointRepository.updateBulkCheckpoint(anyString(), any(), anyLong())).thenReturn(true);
    }

    @Test
    void advancesThroughContiguousCompletedRanges() {
        when(checkpointRepository.getBulkCheckpoint(anyString(), anyString(), anyString()))
                .thenReturn(Optional.empty());

        advancer.advance(migration(), List.of(
                completed("orders", "db1", 0, 1),
                completed("orders", "db1", 1, 2)));

        ArgumentCaptor<TableCheckpoint> captor = ArgumentCaptor.forClass(TableCheckpoint.class);
        verify(checkpointRepository, times(2)).initBulkCheckpoint(anyString(), captor.capture());
        assertThat(captor.getAllValues())
                .extracting(TableCheckpoint::getLastProcessedCreatedAt)
                .containsExactly(DATE_FROM.plusHours(1), DATE_FROM.plusHours(2));
    }

    @Test
    void doesNotJumpOverGapWhenEarlierRangeStillRunning() {
        when(checkpointRepository.getBulkCheckpoint(anyString(), anyString(), anyString()))
                .thenReturn(Optional.empty());

        // Диапазон [0..1) ещё выполняется, завершён только [1..2)
        advancer.advance(migration(), List.of(completed("orders", "db1", 1, 2)));

        verify(checkpointRepository, never()).initBulkCheckpoint(anyString(), any());
        verify(checkpointRepository, never()).updateBulkCheckpoint(anyString(), any(), anyLong());
    }

    @Test
    void tracksSourceShardsIndependently() {
        when(checkpointRepository.getBulkCheckpoint(anyString(), anyString(), anyString()))
                .thenReturn(Optional.empty());

        advancer.advance(migration(), List.of(
                completed("orders", "db1", 0, 1),
                completed("orders", "db2", 0, 1)));

        ArgumentCaptor<TableCheckpoint> captor = ArgumentCaptor.forClass(TableCheckpoint.class);
        verify(checkpointRepository, times(2)).initBulkCheckpoint(anyString(), captor.capture());
        assertThat(captor.getAllValues())
                .extracting(TableCheckpoint::getSourceDb)
                .containsExactlyInAnyOrder("db1", "db2");
    }

    @Test
    void continuesFromExistingCheckpoint() {
        when(checkpointRepository.getBulkCheckpoint(anyString(), anyString(), anyString()))
                .thenReturn(Optional.of(TableCheckpoint.builder()
                        .table("orders")
                        .sourceDb("db1")
                        .lastProcessedCreatedAt(DATE_FROM.plusHours(1))
                        .revision(5L)
                        .build()));

        advancer.advance(migration(), List.of(completed("orders", "db1", 1, 2)));

        verify(checkpointRepository).updateBulkCheckpoint(anyString(), any(), anyLong());
    }

    private static MigrationState migration() {
        return MigrationState.builder()
                .migrationId("mig-1")
                .dateFrom(DATE_FROM)
                .dateTo(DATE_FROM.plusDays(1))
                .currentTopology("v1")
                .targetTopology("v2")
                .build();
    }

    private static MigrationTask completed(String table, String sourceDb, int fromHour, int toHour) {
        return MigrationTask.builder()
                .taskId("task-%s-%s-%d".formatted(table, sourceDb, fromHour))
                .migrationId("mig-1")
                .table(table)
                .tableConfigKey(table)
                .sourceDb(sourceDb)
                .status(TaskStatus.COMPLETED)
                .rangeFrom(DATE_FROM.plusHours(fromHour))
                .rangeTo(DATE_FROM.plusHours(toHour))
                .build();
    }
}
