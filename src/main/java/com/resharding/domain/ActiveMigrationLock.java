package com.resharding.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Блокировка единственной активной migration.
 *
 * <p>Создаётся через atomic {@code put-if-absent} по ключу
 * {@code {etcd.prefix}migration/active-lock}. Гарантирует инвариант:
 * в системе одновременно не более одной migration в {@link MigrationStatus#RUNNING}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActiveMigrationLock {

    /** Идентификатор migration, удерживающей блокировку. */
    private String migrationId;

    /** {@code instanceId} Leader, создавшего блокировку. */
    private String owner;
}
