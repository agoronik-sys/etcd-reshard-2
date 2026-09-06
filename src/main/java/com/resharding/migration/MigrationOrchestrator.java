package com.resharding.migration;

import com.resharding.api.dto.MigrationCommandRequest;
import com.resharding.api.dto.MigrationResponse;
import com.resharding.cluster.LeaderElectionService;
import com.resharding.config.InstanceProperties;
import com.resharding.config.TableMigrationProperties;
import com.resharding.config.TopologyProperties;
import com.resharding.domain.MigrationState;
import com.resharding.domain.MigrationStatus;
import com.resharding.etcd.ActiveMigrationLockRepository;
import com.resharding.etcd.MigrationStateRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.UUID;

/**
 * Оркестратор migration: обработка команд START, STOP, RESUME.
 *
 * <p>Команды принимаются <strong>только Leader'ом</strong>. При START:
 * <ol>
 *   <li>проверяется отсутствие активной RUNNING migration;</li>
 *   <li>захватывается {@link com.resharding.domain.ActiveMigrationLock};</li>
 *   <li>фиксируется {@code T0} — граница bulk/incremental;</li>
 *   <li>сохраняется {@link MigrationState} в {@code etcd}.</li>
 *   <li>Shard-БД здесь не открываются: индексы и данные обрабатываются только после появления Task.</li>
 * </ol>
 *
 * <p>После STOP migration переходит в {@link MigrationStatus#STOPPED};
 * прогресс сохраняется, lock освобождается. RESUME восстанавливает RUNNING.
 */
@Service
public class MigrationOrchestrator {

    private static final DateTimeFormatter MIGRATION_ID_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final MigrationStateRepository migrationStateRepository;
    private final ActiveMigrationLockRepository activeMigrationLockRepository;
    private final LeaderElectionService leaderElectionService;
    private final TopologyProperties topologyProperties;
    private final InstanceProperties instanceProperties;
    private final TableMigrationProperties tableMigrationProperties;

    public MigrationOrchestrator(
            MigrationStateRepository migrationStateRepository,
            ActiveMigrationLockRepository activeMigrationLockRepository,
            LeaderElectionService leaderElectionService,
            TopologyProperties topologyProperties,
            InstanceProperties instanceProperties,
            TableMigrationProperties tableMigrationProperties) {
        this.migrationStateRepository = migrationStateRepository;
        this.activeMigrationLockRepository = activeMigrationLockRepository;
        this.leaderElectionService = leaderElectionService;
        this.topologyProperties = topologyProperties;
        this.instanceProperties = instanceProperties;
        this.tableMigrationProperties = tableMigrationProperties;
    }

    /** Возвращает текущую migration из {@code etcd}. */
    public Optional<MigrationState> getCurrentMigration() {
        return migrationStateRepository.getCurrent();
    }

    /**
     * Обрабатывает команду migration.
     *
     * @param request команда с {@code dateFrom}, {@code dateTo} и типом (START/STOP/RESUME)
     * @return результат: accepted или rejected с причиной
     */
    public MigrationResponse handleCommand(MigrationCommandRequest request) {
        if (!leaderElectionService.isLeader()) {
            return MigrationResponse.rejected("Only leader can process migration commands", getCurrentOrNull());
        }

        return switch (request.getStatus()) {
            case START -> start(request);
            case STOP -> stop();
            case RESUME -> resume();
        };
    }

    private MigrationResponse start(MigrationCommandRequest request) {
        Optional<String> validationError = validateStartRequest(request);
        if (validationError.isPresent()) {
            return MigrationResponse.rejected(validationError.get(), getCurrentOrNull());
        }

        Optional<MigrationState> existing = migrationStateRepository.getCurrent();
        if (existing.isPresent() && existing.get().getStatus() == MigrationStatus.RUNNING) {
            return MigrationResponse.rejected("Active migration already running", existing.get());
        }

        if (existing.isPresent() && existing.get().getStatus() == MigrationStatus.STOPPED) {
            return MigrationResponse.rejected(
                    "Stopped migration exists. Use RESUME or wait until a new migration is allowed",
                    existing.get());
        }

        String migrationId = generateMigrationId();
        if (!activeMigrationLockRepository.tryAcquire(
                migrationId,
                instanceProperties.getInstanceId(),
                leaderElectionService.leaderLeaseId())) {
            return MigrationResponse.rejected("Failed to acquire active migration lock", getCurrentOrNull());
        }

        LocalDateTime now = LocalDateTime.now();
        MigrationState state = MigrationState.builder()
                .migrationId(migrationId)
                .status(MigrationStatus.RUNNING)
                .currentTopology(topologyProperties.getCurrentTopology())
                .targetTopology(topologyProperties.getTargetTopology())
                .dateFrom(request.getDateFrom())
                .dateTo(request.getDateTo())
                .checkBeforeInsert(Boolean.TRUE.equals(request.getCheckBeforeInsert()))
                .t0(now)
                .startedAt(now)
                .updatedAt(now)
                .build();

        migrationStateRepository.save(state);
        /*
         * START намеренно не касается shard-БД. Пулы и DDL создаются только
         * после появления Task и начала реальной data/index операции.
         */
        return MigrationResponse.accepted(state);
    }

    private MigrationResponse stop() {
        Optional<MigrationState> existing = migrationStateRepository.getCurrent();
        if (existing.isEmpty() || existing.get().getStatus() != MigrationStatus.RUNNING) {
            return MigrationResponse.rejected("No running migration to stop", getCurrentOrNull());
        }

        MigrationState state = existing.get();
        state.setStatus(MigrationStatus.STOPPED);
        state.setUpdatedAt(LocalDateTime.now());
        migrationStateRepository.save(state);
        activeMigrationLockRepository.release(state.getMigrationId());
        return MigrationResponse.accepted(state);
    }

    private MigrationResponse resume() {
        Optional<MigrationState> existing = migrationStateRepository.getCurrent();
        if (existing.isEmpty() || existing.get().getStatus() != MigrationStatus.STOPPED) {
            return MigrationResponse.rejected("No stopped migration to resume", getCurrentOrNull());
        }

        if (!activeMigrationLockRepository.tryAcquire(
                existing.get().getMigrationId(),
                instanceProperties.getInstanceId(),
                leaderElectionService.leaderLeaseId())) {
            return MigrationResponse.rejected("Failed to acquire active migration lock", existing.get());
        }

        MigrationState state = existing.get();
        state.setStatus(MigrationStatus.RUNNING);
        state.setUpdatedAt(LocalDateTime.now());
        migrationStateRepository.save(state);
        return MigrationResponse.accepted(state);
    }

    /**
     * Проверяет обязательные поля START: без {@code dateFrom}/{@code dateTo}
     * планировщик не может построить ни одного диапазона.
     */
    private Optional<String> validateStartRequest(MigrationCommandRequest request) {
        if (request.getDateFrom() == null || request.getDateTo() == null) {
            return Optional.of("dateFrom and dateTo are required for START");
        }
        if (!request.getDateFrom().isBefore(request.getDateTo())) {
            return Optional.of("dateFrom must be strictly before dateTo");
        }
        for (var entry : tableMigrationProperties.getEnabledTables().entrySet()) {
            if (!entry.getValue().effectiveShardKey().isUnique()) {
                return Optional.of(
                        "Table %s must declare shardKey.unique=true: migration ledger deduplicates by hash key"
                                .formatted(entry.getKey()));
            }
        }
        return Optional.empty();
    }

    private MigrationState getCurrentOrNull() {
        return migrationStateRepository.getCurrent().orElse(null);
    }

    private String generateMigrationId() {
        return "mig-" + LocalDateTime.now().format(MIGRATION_ID_FORMAT)
                + "-" + UUID.randomUUID().toString().substring(0, 12);
    }
}
