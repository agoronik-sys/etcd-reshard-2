package com.resharding.worker;

import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationTask;
import org.springframework.stereotype.Component;

/**
 * Определяет, нужен ли уменьшенный batch для retry/check-before-insert.
 *
 * <p>PK source не переносится — при повторном выполнении Task после падения Pod
 * часть строк могла быть уже скопирована.
 *
 * <p>Сама запись всегда идемпотентна через migration-ledger. Политика выбирает
 * консервативный размер batch, когда:
 * <ul>
 *   <li>Task была взята повторно ({@code generation > 1});</li>
 *   <li>migration запущена с флагом {@link MigrationState#isCheckBeforeInsert()}.</li>
 * </ul>
 *
 * <p>На чистом первом проходе ledger также используется, но разрешён большой batch.
 */
@Component
public class InsertGuardPolicy {

    /**
     * {@code true}, если требуется уменьшенный idempotent batch.
     */
    public boolean requiresIdempotentInsert(MigrationTask task, MigrationState migration) {
        if (migration != null && migration.isCheckBeforeInsert()) {
            return true;
        }
        return task.isReclaimed();
    }
}
