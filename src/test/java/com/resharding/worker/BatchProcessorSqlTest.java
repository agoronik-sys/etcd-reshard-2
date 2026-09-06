package com.resharding.worker;

import com.resharding.config.ReshardingRootProperties;
import com.resharding.domain.MigrationTask;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Фиксирует SQL-семантику keyset pagination.
 *
 * <p>Соседние Task используют полуинтервалы [from,to), поэтому верхняя граница
 * строго исключена. Первый запрос не использует synthetic lastId=0: иначе строки
 * с нулевым или отрицательным source PK на rangeFrom были бы потеряны.
 */
class BatchProcessorSqlTest {

    private final BatchProcessor processor = new BatchProcessor(null, null, null, null, null);

    @Test
    void initialQueryUsesHalfOpenRangeWithoutSyntheticIdCursor() {
        String sql = processor.buildSelectSql(task(), table(), false);

        assertThat(sql).contains("\"created_at\" >= ?");
        assertThat(sql).contains("\"created_at\" < ?");
        assertThat(sql).doesNotContain("(\"created_at\", \"id\") >");
        assertThat(sql).contains("SELECT *");
    }

    @Test
    void continuationUsesCompositeKeysetAndExclusiveUpperBound() {
        String sql = processor.buildSelectSql(task(), table(), true);

        assertThat(sql).contains("(\"created_at\", \"id\") > (?, ?)");
        assertThat(sql).contains("\"created_at\" < ?");
        assertThat(sql).doesNotContain("<=");
    }

    private MigrationTask task() {
        return MigrationTask.builder().table("orders").build();
    }

    private ReshardingRootProperties.TableConfig table() {
        ReshardingRootProperties.TableConfig table = new ReshardingRootProperties.TableConfig();
        table.setRangeColumn("created_at");
        table.setIdColumn("id");
        return table;
    }
}
