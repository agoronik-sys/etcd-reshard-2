package com.resharding.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Статус создания verify-индексов на новом DB shard для таблицы.
 *
 * <p>Ключ: {@code {etcd.prefix}migration/{migrationId}/indexes/{dbId}/{table}}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableIndexStatus {

    private String migrationId;
    private String dbId;
    private String table;
    private List<String> indexNames;
    private IndexStatus status;
    private LocalDateTime createdAt;

    /**
     * Ключ партиции для партиционированных таблиц ({@code 2024-01}).
     * {@code null} — индексы на всю таблицу (непартиционированный случай).
     */
    private String partitionKey;

    public enum IndexStatus {
        PENDING,
        CREATED,
        FAILED
    }
}
