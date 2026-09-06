package com.resharding.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.resharding.etcd.JacksonConfiguration;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Регрессия: производный геттер {@code isReclaimed()} не должен попадать в JSON.
 * Иначе чтение Task из etcd падало с {@code UnrecognizedPropertyException}.
 */
/**
 * Регрессия формата MigrationTask, сохраняемого в etcd.
 *
 * <p>Производные Java-bean методы вроде {@code isReclaimed()} не должны
 * становиться JSON-полями без setter: старые записи после обновления приложения
 * иначе перестают десериализоваться. Тесты также фиксируют forward compatibility.
 */
class MigrationTaskJsonTest {

    private final ObjectMapper mapper = new JacksonConfiguration().objectMapper();

    @Test
    void reclaimedFlagIsNotSerialized() throws Exception {
        MigrationTask task = MigrationTask.builder()
                .taskId("task-orders-db1-20240101T000000")
                .generation(2)
                .build();

        assertThat(mapper.writeValueAsString(task)).doesNotContain("reclaimed");
    }

    @Test
    void roundTripPreservesTaskState() throws Exception {
        MigrationTask task = MigrationTask.builder()
                .taskId("task-orders-db1-20240101T000000")
                .migrationId("mig-20240101-001")
                .table("orders")
                .tableConfigKey("orders")
                .sourceDb("db1")
                .currentTopology("v1")
                .targetTopology("v2")
                .rangeFrom(LocalDateTime.of(2024, 1, 1, 0, 0))
                .rangeTo(LocalDateTime.of(2024, 1, 1, 1, 0))
                .status(TaskStatus.RUNNING)
                .generation(2)
                .sequenceNumber(7)
                .build();

        MigrationTask back = mapper.readValue(mapper.writeValueAsString(task), MigrationTask.class);

        assertThat(back.getTaskId()).isEqualTo(task.getTaskId());
        assertThat(back.getRangeFrom()).isEqualTo(task.getRangeFrom());
        assertThat(back.getGeneration()).isEqualTo(2);
        assertThat(back.isReclaimed()).isTrue();
    }

    @Test
    void unknownFieldFromOlderVersionIsIgnored() throws Exception {
        String json = """
                {"taskId":"task-1","generation":1,"legacyField":"value"}
                """;

        MigrationTask back = mapper.readValue(json, MigrationTask.class);

        assertThat(back.getTaskId()).isEqualTo("task-1");
        assertThat(back.isReclaimed()).isFalse();
    }
}
