package com.resharding.api;

import com.resharding.api.dto.MigrationCommandRequest;
import com.resharding.api.dto.MigrationResponse;
import com.resharding.domain.MigrationState;
import com.resharding.migration.MigrationOrchestrator;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API управления migration.
 *
 * <p>Эндпоинты:
 * <ul>
 *   <li>{@code GET  /api/v1/migration} — текущее состояние migration;</li>
 *   <li>{@code POST /api/v1/migration} — команда START / STOP / RESUME.</li>
 * </ul>
 *
 * <p>Команды обрабатываются только Leader'ом; при обращении к non-leader Pod
 * возвращается HTTP 409 Conflict.
 */
@RestController
@RequestMapping("/api/v1/migration")
public class MigrationController {

    private final MigrationOrchestrator migrationOrchestrator;

    public MigrationController(MigrationOrchestrator migrationOrchestrator) {
        this.migrationOrchestrator = migrationOrchestrator;
    }

    /** Возвращает текущую migration или {@code 204 No Content}. */
    @GetMapping
    public ResponseEntity<MigrationState> getCurrent() {
        return migrationOrchestrator.getCurrentMigration()
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.noContent().build());
    }

    /**
     * Выполняет команду migration.
     *
     * @param request тело запроса с {@code status: START|STOP|RESUME} и опциональными {@code dateFrom/dateTo}
     * @return {@code 200 OK} при успехе, {@code 409 Conflict} при отклонении
     */
    @PostMapping
    public ResponseEntity<MigrationResponse> command(@Valid @RequestBody MigrationCommandRequest request) {
        MigrationResponse response = migrationOrchestrator.handleCommand(request);
        return response.isAccepted()
                ? ResponseEntity.ok(response)
                : ResponseEntity.status(409).body(response);
    }
}
