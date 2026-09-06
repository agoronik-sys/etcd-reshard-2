package com.resharding.api.dto;

import com.resharding.domain.MigrationState;
import lombok.Builder;
import lombok.Data;

/**
 * Ответ REST API на команду migration.
 */
@Data
@Builder
public class MigrationResponse {

    /** {@code true} если команда принята и выполнена. */
    private boolean accepted;

    /** Причина отклонения; {@code null} при успехе. */
    private String message;

    /** Актуальное состояние migration после выполнения команды. */
    private MigrationState migration;

    /** Успешный ответ с состоянием migration. */
    public static MigrationResponse accepted(MigrationState migration) {
        return MigrationResponse.builder()
                .accepted(true)
                .migration(migration)
                .build();
    }

    /** Ответ об отклонении команды с пояснением. */
    public static MigrationResponse rejected(String message, MigrationState migration) {
        return MigrationResponse.builder()
                .accepted(false)
                .message(message)
                .migration(migration)
                .build();
    }
}
