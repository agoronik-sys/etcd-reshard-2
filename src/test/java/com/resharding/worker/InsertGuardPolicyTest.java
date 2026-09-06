package com.resharding.worker;

import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationTask;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет выбор консервативного ledger batch.
 *
 * <p>Все INSERT уже защищены ledger; policy отвечает именно за снижение размера
 * пачки при reclaim или явном checkBeforeInsert, когда вероятность повторного
 * прохода и нагрузка от конфликтов выше.
 */
class InsertGuardPolicyTest {

    private final InsertGuardPolicy policy = new InsertGuardPolicy();

    @Test
    void plainInsertOnFirstClaim() {
        MigrationTask task = MigrationTask.builder().generation(1).build();

        assertThat(policy.requiresIdempotentInsert(task, null)).isFalse();
    }

    @Test
    void idempotentInsertOnReclaimedTask() {
        MigrationTask task = MigrationTask.builder().generation(2).build();

        assertThat(policy.requiresIdempotentInsert(task, null)).isTrue();
    }

    @Test
    void idempotentInsertWhenMigrationFlagEnabled() {
        MigrationTask task = MigrationTask.builder().generation(1).build();
        MigrationState migration = MigrationState.builder().checkBeforeInsert(true).build();

        assertThat(policy.requiresIdempotentInsert(task, migration)).isTrue();
    }
}
