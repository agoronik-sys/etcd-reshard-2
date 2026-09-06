# etcd-reshard

Распределённый сервис автоматического решардирования данных между инстансами PostgreSQL.

Координация, Leader Election, состояние migration и распределение задач — через **etcd**. Каждый Pod может быть Leader или Worker. Количество Pod динамическое.

---

## Содержание

- [Цель](#цель)
- [Архитектура](#архитектура)
- [Стек](#стек)
- [Быстрый старт](#быстрый-старт)
- [Конфигурация](#конфигурация)
- [REST API](#rest-api)
- [Структура ключей etcd](#структура-ключей-etcd)
- [Жизненный цикл migration](#жизненный-цикл-migration)
- [Жизненный цикл соединений к shard-БД](#жизненный-цикл-соединений-к-shard-бд)
- [Структура проекта](#структура-проекта)
- [Статус реализации](#статус-реализации)
- [Тесты](#тесты)
- [Ключевые инварианты](#ключевые-инварианты)
- [Открытые вопросы](#открытые-вопросы)

---

## Цель

При переходе с одной topology на другую сервис автоматически:

1. Определяет, какие записи должны находиться в других shard'ах
2. Копирует их в target DB
3. Проверяет успешность копирования
4. Удаляет записи из source DB **только после подтверждённого verify**

Пример:

```text
currentTopology = v1   →   db1, db2
targetTopology  = v2   →   db1, db2, db3, db4
```

Сервис работает независимо от механизма маршрутизации пользовательских запросов (Nginx + Ketama). Его задача — привести **физическое расположение данных** к `targetTopology`.

---

## Архитектура

```text
                         REST API
                             |
                             v
                  +----------------------+
                  |   Migration Service  |
                  |   (Spring Boot Pod)  |
                  +----------+-----------+
                             |
                             v
                       +-----------+
                       |   etcd    |
                       |-----------|
                       | Leader    |
                       | Members   |
                       | Migration |
                       | Tasks     |
                       | Checkpoint|
                       | Locks     |
                       | Leases    |
                       +-----+-----+
                             |
                    Leader Scheduler
                             |
             +---------------+---------------+
             |               |               |
             v               v               v
         Worker 1        Worker 2        Worker N
             |               |               |
             +---------------+---------------+
                             |
                     Source / Target DB
                     (PostgreSQL + HikariCP)
```

### Роли Pod

| Pod | Роль |
|-----|------|
| 1 из N | **Leader** — управляет migration, создаёт Task, двигает checkpoint |
| остальные | **Worker** — выполняют Task (max 1 Task на Pod) |

```text
3 Pod  → 1 Leader + 2 Worker
6 Pod  → 1 Leader + 5 Worker
10 Pod → 1 Leader + 9 Worker
```

Число Worker **не захардкожено** — определяется из активных members в etcd.

### Sliding Window

В etcd хранится только:

```text
checkpoint
   +--- Task 101  (RUNNING)
   +--- Task 102  (RUNNING)
   +--- Task 103  (CREATED)
```

Диапазоны **не создаются заранее** (не миллионы ключей). После завершения Task — checkpoint сдвигается, создаётся следующий диапазон.

---

## Стек

| Компонент | Версия / технология |
|-----------|---------------------|
| Java | 21 |
| Spring Boot | 3.3.5 |
| etcd client | jetcd 0.7.7 |
| DB | PostgreSQL |
| Connection pool | HikariCP (`minimumIdle=1`, `maximumPoolSize=10`), lazy — см. [Жизненный цикл соединений](#жизненный-цикл-соединений-к-shard-бд) |
| Build | Maven |

---

## Быстрый старт

### Требования

- JDK 21
- Maven 3.8+
- etcd (локально или кластер)
- PostgreSQL instances (db1..db4)

### Сборка и тесты

```bash
mvn compile
mvn test
mvn package -DskipTests
```

### Запуск

```bash
java -jar target/etcd-reshard-1.0.0-SNAPSHOT.jar
```

Сервис слушает порт **8080**.

### Запуск migration

```bash
curl -X POST http://localhost:8080/api/v1/migration \
  -H "Content-Type: application/json" \
  -d '{
    "dateFrom": "2023-01-01T00:00:00",
    "dateTo": "2026-09-01T00:00:00",
    "checkBeforeInsert": false,
    "status": "START"
  }'
```

> Команды принимаются **только Leader Pod**. При обращении к non-leader — HTTP 409.

---

## Конфигурация

Основной файл: `src/main/resources/application.yml`

### DB instances

```yaml
segments:
  database:
    db1:
      url: jdbc:postgresql://localhost:5432/db1
      username: postgres
      password: postgres
      ketamaServer: db1          # строка server в nginx upstream
      ketamaWeight: 1
    db3:
      url: jdbc:postgresql://localhost:5432/db3
      ketamaServer: postgres3.example.com:5432
```

### Topology

```yaml
topologies:
  v1:
    version: v1
    shards: [db1, db2]
  v2:
    version: v2
    shards: [db1, db2, db3, db4]

currentTopology: v1
targetTopology: v2
```

### Migration

```yaml
migration:
  maxConcurrentTasks: 5
  taskPerWorker: 1
  copyBatchSize: 50000            # COPY на новые shard (CopyManager — планируется)
  idempotentInsertBatchSize: 500  # уменьшенный ledger INSERT при reclaim
  activeSourceBatchDivisor: 10    # ÷10 для db1/db2 (рабочая БД, autovacuum)
  rangeDurationMinutes: 60
  incrementalCycleSeconds: 10
  schedulerIntervalMs: 2000
  poolReleaseGraceSeconds: 60
  ledger:
    schema: public
    tableName: reshard_migration_ledger
    autoCreate: true
```

Размер batch вычисляет `BatchSizeResolver`:

| Режим | READ (LIMIT) | WRITE |
|-------|-------------|-------|
| Идемпотентный INSERT (reclaim / `checkBeforeInsert`) | 500 | 500 |
| COPY → новый shard (db3, db4) | 50 000* | 50 000 |
| COPY, source = active shard (db1, db2) | 5 000* | 50 000 → db3; 5 000 → db1/db2 |

\*50 000 / `activeSourceBatchDivisor` (10) при чтении с production source.

Per-table override: `tables.orders.batchSize: 30000` (если `0` — глобальный `copyBatchSize`).

### Лимиты нагрузки на DB

```yaml
databaseLimits:
  db1:
    maxSourceTasks: 2
    maxTargetTasks: 2
```

### Таблицы

```yaml
tables:
  orders:
    enabled: true
    tableName: orders
    shardKey:
      defaultField: op_id
      unique: true              # обязательно: hash key глобально уникален
      rules:
        - whenField: eq
          whenValue: b12
          useField: mb_uid
    shardAlgorithm: KETAMA
    rangeStrategy: TIME
    rangeColumn: created_at
    idColumn: id
    businessKey: [external_id]
    verifyDateColumns: [created_at]
    verifyCompositeWithId: true
  users:
    enabled: true
    tableName: users
    shardKey:
      field: account_id
    shardAlgorithm: KETAMA
    businessKey: [business_uuid]
  events:
    enabled: false               # исключена из migration
    tableName: events
    shardKey:
      field: client_id
    partition:                   # партиционирование по месяцам
      enabled: true
      strategy: MONTHLY
      namePattern: "{table}_{yyyy}_{MM}"
      indexAfterPartitionComplete: true
```

| Поле | Описание |
|------|----------|
| `enabled` | Включена ли таблица (default `true`) |
| `tableName` | Имя в БД (если не задано — ключ конфигурации) |
| `shardKey.field` | Одно поле для hash |
| `shardKey.defaultField` + `rules` | Условный выбор поля hash |
| `shardKey.unique` | Подтверждение глобальной уникальности выбранного hash key; обязательно для ledger |
| `shardAlgorithm` | `KETAMA` (nginx, default) или `HASH_MOD` |
| `rangeColumn` / `idColumn` | Keyset pagination |
| `businessKey` | Ключ для verify (обязателен при check-before-insert) |
| `verifyDateColumns` | Date-поля для verify-индексов |
| `batchSize` | Override `copyBatchSize` для таблицы (`0` = глобальный) |
| `partition.*` | Помесячные партиции; индексы после залития месяца |

### Ketama (nginx consistent hash)

Target DB для каждой строки определяется через Ketama — тот же алгоритм, что nginx `hash $key consistent` (CRC32, 160 points/server).

**Важно:** `ketamaServer` должен **точно совпадать** со строкой server в nginx upstream.

Pipeline при чтении batch:

```text
SELECT ... FROM source (keyset, LIMIT = BatchSizeResolver)
    ↓
ShardKeyResolver → shard key field + value
    ↓
Ketama.locate() / HASH_MOD → target db
    ↓
если target ≠ source → INSERT → INDEXES → VERIFY → DELETE
```

### etcd

```yaml
etcd:
  endpoints:
    - http://localhost:2379
  prefix: segments/
  lease-ttl-seconds: 30
```

---

## REST API

| Метод | URL | Описание |
|-------|-----|----------|
| `GET` | `/api/v1/migration` | Текущее состояние migration |
| `POST` | `/api/v1/migration` | Команда START / STOP / RESUME |

### Команды

| status | Действие |
|--------|----------|
| `START` | Запустить migration; фиксирует T0; создаёт индексы на новых shard |
| `STOP` | Остановить; Leader прекращает создание Task |
| `RESUME` | Возобновить STOPPED migration с checkpoint |

### Поля START

| Поле | Описание |
|------|----------|
| `dateFrom` / `dateTo` | Исторический диапазон bulk migration |
| `checkBeforeInsert` | `true` — уменьшенный ledger batch на всю migration |
| `status` | `START` |

### Пример ответа

```json
{
  "accepted": true,
  "migration": {
    "migrationId": "mig-20260906-001",
    "status": "RUNNING",
    "currentTopology": "v1",
    "targetTopology": "v2",
    "dateFrom": "2023-01-01T00:00:00",
    "dateTo": "2026-09-01T00:00:00",
    "checkBeforeInsert": false,
    "t0": "2026-09-06T12:00:00",
    "startedAt": "2026-09-06T12:00:00"
  }
}
```

---

## Структура ключей etcd

Prefix: `etcd.prefix` (default `segments/`).

| Ключ | Назначение |
|------|------------|
| `segments/leader` | Текущий Leader (lease) |
| `segments/members/{instanceId}` | Регистрация Pod (lease) |
| `segments/migration/current` | Состояние текущей migration |
| `segments/migration/active-lock` | Блокировка единственной активной migration |
| `segments/migration/{id}/checkpoints/{table}/{sourceDb}` | Bulk checkpoint пары таблица×shard (CAS) |
| `segments/migration/{id}/incremental/{table}/checkpoint` | Incremental checkpoint |
| `segments/migration/{id}/tasks/{taskId}` | Task (без lease — переживает падение Worker) |
| `segments/migration/{id}/indexes/{dbId}/{table}` | Статус verify-индексов (вся таблица) |
| `segments/migration/{id}/indexes/{dbId}/{table}/{2024-01}` | Статус индексов на партиции |
| `segments/migration/{id}/locks/task/{taskId}` | Ownership Task (lease + keepalive) |
| `segments/slots/source/{dbId}/{slotId}` | Слот нагрузки source DB |
| `segments/slots/target/{dbId}/{slotId}` | Слот нагрузки target DB |

**В etcd не хранятся:** бизнес-данные, все PK, миллионы диапазонов, история Task.

---

## Жизненный цикл migration

```text
REST START
    ↓
active migration lock (put-if-absent) + T0
    ↓
Leader: sliding window Task
    ↓
Worker pipeline (см. ниже)
    ↓
Leader: checkpoint → удалить Task из etcd
    ↓
dateTo достигнут + incremental догнан → COMPLETED
```

### Pipeline Worker (текущая реализация)

```text
claim Task: одна etcd txn (Task revision + lease-lock + generation/token)
    ↓
┌── цикл по диапазону, пока прочитано == readBatchSize ──────────┐
│                                                                │
│  READ source batch (keyset, LIMIT = BatchSizeResolver)         │
│      ↓                                                         │
│  ShardKeyResolver → shard key (простой / условный)             │
│      ↓                                                         │
│  ShardCalculator (KETAMA / HASH_MOD) → target shard            │
│      ↓                                                         │
│  если target = source → skip                                   │
│      ↓                                                         │
│  TX: INSERT migration-ledger + INSERT target                   │
│    · source PK не включается в бизнес-таблицу                  │
│    · identity = table + hash field + hash value                │
│      ↓                                                         │
│  ensureBeforeVerify: индексы на новых shard / партиции месяца   │
│      ↓                                                         │
│  VERIFY по date-полям        ← 🔶 hook (лог), JDBC нет         │
│      ↓                                                         │
│  fencing: Task revision + exact claim token → CAS progress      │
│                                                                │
└────────────────────────────────────────────────────────────────┘
    ↓
COMPLETED (диапазон исчерпан)
    ↓
DELETE source                 ← ❌ не реализовано
```

Один batch **не** завершает Task: иначе строки за пределами первого batch
остались бы неперенесёнными, а checkpoint уехал бы на `rangeTo`.
Прогресс сохраняется после каждого batch, поэтому переназначенная Task
продолжает с последней позиции, а не с начала диапазона.

### Verify-индексы

При `v1 [db1, db2]` → `v2 [db1, db2, db3, db4]` индексы создаются **только на db3 и db4**.

**Непартиционированные таблицы** — лениво, при первой Task, реально
направившей строку на новый shard. `START` не открывает подключения к БД:

```sql
CREATE INDEX IF NOT EXISTS idx_orders_verify_created_at ON orders (created_at);
CREATE INDEX IF NOT EXISTS idx_orders_verify_created_at_id ON orders (created_at, id);
```

**Партиционированные** — после полного залития месяца на все новые shard:

```sql
CREATE INDEX IF NOT EXISTS idx_orders_2024_01_verify_created_at ON orders_2024_01 (created_at);
```

Leader определяет завершение месяца через `PartitionCompletionDetector` при продвижении checkpoint.

### Защита от дублей (reclaim)

PK source **не переносится** и не используется как глобальная identity:
одинаковый PK допустим на db1 и db2. Каждое hash-поле обязано быть глобально
уникальным (`shardKey.unique: true`). На каждом target создаётся ledger:

```sql
PRIMARY KEY (migration_id, source_table, shard_key_field, shard_key_value)
```

Ledger и бизнес-строка записываются в **одной PostgreSQL-транзакции**.
Повторный проход получает `ON CONFLICT DO NOTHING` в ledger и не создаёт дубль.
Если INSERT бизнес-строки падает, ledger откатывается вместе с ним. Имя hash-поля
входит в ключ, поскольку условная маршрутизация может выбирать `op_id` или `mb_uid`.

### Checkpoint

- **Bulk:** `lastProcessedCreatedAt` + `lastProcessedId` — keyset: `WHERE (created_at, id) > (:last, :lastId)`
- Ведётся отдельно для **каждой пары (таблица × source shard)**: db1 и db2 переносятся независимо
- Сдвигается **только Leader'ом** через CAS
- **Непрерывность проверяется по времени, не по номеру Task:** checkpoint сдвигается на `rangeTo` завершённой Task только если её `rangeFrom` совпадает с текущей позицией. Если Task `[10:00–11:00)` ещё выполняется, а `[11:00–12:00)` завершена — checkpoint остаётся на 10:00
- Completed Task удаляется только после проверки persisted
  `checkpoint >= task.rangeTo`; gap или CAS conflict сохраняет Task для retry
- **Incremental:** отдельный checkpoint после T0 (ключ в etcd; pipeline — skeleton)

### Ownership Task и переназначение

Lease висит на **отдельном lock-ключе**, а не на ключе Task:

```text
tasks/{taskId}            — без lease, переживает падение Pod
locks/task/{taskId}       — lease + keepalive, пока Worker работает
```

Если lease был бы на самой Task, падение Pod удалило бы её вместе с диапазоном.
При падении Worker истекает только lock. Leader conditional-CAS переводит Task
в `AVAILABLE`, только если lock действительно отсутствует и revision не изменилась.
Следующий claim увеличивает `generation` и создаёт новый случайный `claimToken`.
Старый Worker не сможет сохранить cursor: progress CAS проверяет одновременно
Task revision и точное значение lease-lock.

`taskId` детерминирован (`task-{table}-{sourceDb}-{rangeFrom}`), поэтому повторное
планирование того же диапазона перезаписывает тот же ключ, в том числе после смены Leader.

Active-migration lock привязан к lease текущего Leader. После failover старый lock
исчезает вместе с lease, новый Leader восстанавливает его для той же RUNNING migration.
При `STOP` и `COMPLETED` lock удаляется conditional CAS.

### Условие завершения migration

Migration переходит в `COMPLETED` только когда:

1. в etcd нет незавершённых Task, **и**
2. checkpoint каждой пары (таблица × source shard) достиг `dateTo` (`BulkCompletionEvaluator`), **и**
3. incremental догнан (`isCaughtUp` — пока заглушка)

Пустой список Task сам по себе ничего не доказывает: задачи могли не создаться
из-за отсутствия Worker или исчерпанных лимитов DB.

---

## Жизненный цикл соединений к shard-БД

Pod **не держит соединений к shard-БД, пока нет активной migration**. Сервис
разворачивается на десятках Pod, и постоянные пулы к каждому shard исчерпали бы
`max_connections` на стороне PostgreSQL ещё до начала работы.

Реализовано двумя компонентами:

| Компонент | Ответственность |
|---|---|
| `DataSourceRegistry` | Ленивое создание `HikariDataSource` при первом обращении к shard; gate `activate()` / `deactivate()` |
| `DataSourceLifecycleManager` | Сверяет статус migration с состоянием пулов и освобождает соединения |

Правила:

1. Конструктор `DataSourceRegistry` **не открывает ни одного пула** — только запоминает конфигурацию.
2. `get(dbId)` создаёт пул при первом обращении и лишь когда реестр активирован;
   иначе бросает `DataSourceInactiveException` (это ошибка порядка вызовов, а не проблема БД).
3. `START` и `RESUME` shard-БД не касаются. Worker активирует доступ после
   атомарного claim Task; Leader открывает соединение только для index-операции,
   связанной с уже созданной/завершённой Task.
4. Активная DB-операция удерживает `pin`: `deactivate()` не закрывает Hikari pool
   посередине JDBC batch. Закрытие выполняется после `endOperation()` в `finally`.
5. Активация сама по себе соединений не открывает: это только разрешение. Реальный пул
   поднимается первым `get()`, то есть на Pod, который действительно взял Task.
6. Когда migration не в статусе `RUNNING`, пулы закрываются через grace-период
   `migration.pool-release-grace-seconds` (по умолчанию 60 с). Grace защищает от
   пересоздания пулов при паузах между Task и кратковременной недоступности etcd.

Worker проверяет статус migration после каждого batch. При STOP он fenced-CAS
сохраняет cursor, возвращает Task в `AVAILABLE`, освобождает lock и только затем
снимает pin пула. Worker, Leader и lifecycle используют независимые scheduler threads.

---

## Структура проекта

```text
src/main/java/com/resharding/
├── ReshardingApplication.java
├── api/
│   ├── MigrationController.java
│   └── dto/                          # MigrationCommandRequest, MigrationResponse
├── cluster/
│   ├── LeaderElectionService.java    # Leader Election + lease
│   └── MemberRegistryService.java    # Регистрация Pod
├── config/
│   ├── ReshardingRootProperties.java # topologies, tables, databaseLimits
│   ├── MigrationProperties.java    # batch sizes, scheduler
│   ├── EtcdProperties.java         # prefix, lease TTL
│   └── ...
├── db/
│   ├── DataSourceRegistry.java     # HikariCP per shard, lazy
│   ├── DataSourceLifecycleManager.java # Пулы только при активной migration
│   ├── TargetIndexEnsurer.java     # Verify-индексы (таблица / партиция)
│   ├── TargetRowInserter.java      # transactional ledger + business INSERT
│   ├── PartitionNameResolver.java  # orders_2024_01
│   └── PartitionCompletionDetector.java
├── domain/                         # MigrationState, MigrationTask, Checkpoint, InsertMode, ...
├── etcd/
│   ├── EtcdClientFacade.java       # jetcd: CAS, lease, prefix
│   ├── EtcdKeyPaths.java
│   └── *Repository.java
├── leader/
│   ├── LeaderScheduler.java        # Основной tick Leader
│   ├── TaskPlanner.java            # Sliding window от checkpoint
│   ├── CheckpointAdvancer.java     # Непрерывный checkpoint по времени
│   ├── BulkCompletionEvaluator.java# Условие завершения bulk
│   ├── PartitionIndexCoordinator.java
│   ├── IncrementalCycleRunner.java # Incremental tick (skeleton)
│   └── slot/DatabaseSlotManager.java
├── migration/
│   ├── MigrationOrchestrator.java  # START / STOP / RESUME
│   ├── ShardCalculator.java        # KETAMA + HASH_MOD
│   ├── ShardKeyResolver.java       # Условный shard key
│   ├── NewShardResolver.java       # db3, db4 при v1→v2
│   └── ketama/
│       ├── NginxKetamaHashRing.java
│       └── KetamaRingFactory.java
└── worker/
    ├── WorkerTaskClaimService.java # Discovery из etcd + lock lease + keepalive
    ├── WorkerExecutor.java         # Цикл batch по всему диапазону Task
    ├── BatchProcessor.java         # Один batch: READ → INSERT → index/verify
    ├── BatchResult.java            # Курсор + счётчики строк
    ├── BatchSizeResolver.java      # 500 / 5000 / 50000
    ├── BatchSizing.java
    └── InsertGuardPolicy.java
```

---

## Статус реализации

Легенда: ✅ реализовано · 🔶 skeleton / частично · ❌ не реализовано

### Координация и etcd

| Компонент | Статус |
|-----------|--------|
| Leader Election + lease | ✅ `LeaderElectionService` |
| Регистрация Pod + lease | ✅ `MemberRegistryService` |
| Динамическое число Worker | ✅ |
| Ключи etcd (prefix из конфига) | ✅ `EtcdKeyPaths` |
| CAS / put-if-absent | ✅ `EtcdClientFacade` |
| Active migration lock | ✅ |
| Migration state в etcd | ✅ |
| Task (sliding window) | ✅ |
| Bulk / incremental checkpoint (ключи) | ✅ |
| DB source concurrency slots + failover reconciliation | ✅ `DatabaseSlotManager` |
| Verify-index status в etcd | ✅ (+ per-partition key) |

### Migration lifecycle

| Компонент | Статус |
|-----------|--------|
| REST START / STOP / RESUME | ✅ |
| `checkBeforeInsert` в START | ✅ |
| Только одна активная migration | ✅ |
| Фиксация T0 | ✅ |
| Только Leader управляет | ✅ |
| Sliding window от checkpoint | ✅ `TaskPlanner` |
| Task на каждый source shard | ✅ `TaskPlanner` |
| Детерминированные `taskId` | ✅ `task-{table}-{db}-{rangeFrom}` |
| Непрерывный checkpoint (по времени) | ✅ `CheckpointAdvancer` |
| Checkpoint per (таблица × shard) | ✅ `CheckpointRepository` |
| Обнаружение Task через etcd | ✅ `WorkerTaskClaimService.tryClaimNext` |
| Lease на lock-ключе + keepalive | ✅ `WorkerTaskClaimService` |
| Release stale Task по истечении lock | ✅ `LeaderScheduler` |
| Task fencing (generation + token + modRevision CAS) | ✅ |
| 1 Pod = max 1 Task | ✅ |
| maxConcurrentTasks + databaseLimits | ✅ |
| COMPLETED по checkpoint всех shard | ✅ `BulkCompletionEvaluator` |
| Валидация `dateFrom`/`dateTo` | ✅ `MigrationOrchestrator` |

### Routing и shard key

| Компонент | Статус |
|-----------|--------|
| KETAMA (nginx CRC32, 160 pts) | ✅ `NginxKetamaHashRing` |
| HASH_MOD (fallback) | ✅ `ShardCalculator` |
| Условный shard key (rules) | ✅ `ShardKeyResolver` |
| `enabled` / `tableName` в конфиге таблиц | ✅ |
| Ketama server/weight per DB | ✅ |

### Worker / Data pipeline

| Компонент | Статус |
|-----------|--------|
| Claim Task с lease | ✅ |
| Keyset pagination (без OFFSET) | ✅ `BatchProcessor` |
| Цикл batch по всему диапазону Task | ✅ `WorkerExecutor.runBatchLoop` |
| Сохранение прогресса после batch | ✅ `WorkerExecutor` |
| Защита от незакрытого keyset-курсора | ✅ `assertCursorAdvanced` |
| HikariCP per shard | ✅ |
| Пулы поднимаются только при Task/index operation активной migration | ✅ |
| Batch size по режиму (500/5k/50k) | ✅ `BatchSizeResolver` |
| INSERT на target (JDBC) | ✅ `TargetRowInserter` |
| Source PK не включается в INSERT | ✅ `excludeColumn = idColumn` |
| Transactional migration-ledger по глобальному hash key | ✅ |
| COPY / CopyManager (bulk 50k) | ❌ batch size готов, реализация — нет |
| CREATE INDEXES (новые shard, после появления Task) | ✅ `TargetIndexEnsurer` |
| CREATE INDEXES (партиция после месяца) | ✅ `PartitionIndexCoordinator` |
| VERIFY по date-полям (JDBC) | 🔶 hook + лог в `BatchProcessor` |
| DELETE source | ❌ |
| Incremental pipeline после T0 | 🔶 tick в `IncrementalCycleRunner` |
| Полный набор колонок при JDBC INSERT | ✅ `SELECT *`, source PK исключается |

### Конфигурация

| Компонент | Статус |
|-----------|--------|
| DB instances + ketamaServer | ✅ |
| Topologies v1→v2→... | ✅ |
| Per-table: shard key, partition, business key | ✅ |
| databaseLimits | ✅ |
| migration.* (batch sizes, scheduler) | ✅ |

---

## Тесты

```bash
mvn test
```

44 unit-теста + 3 Testcontainers integration-теста, все проходят.

| Тест | Покрытие |
|------|----------|
| `TaskPlannerTest` | окно от checkpoint, все source shard, отсутствие перекрытий, детерминированный `taskId` |
| `CheckpointAdvancerTest` | непрерывность по времени, независимость shard, CAS |
| `MigrationTaskJsonTest` | регрессия: `isReclaimed()` не попадает в JSON |
| `NginxKetamaHashRingTest` | Ketama ring, совместимость с nginx |
| `InsertGuardPolicyTest` | reclaim / checkBeforeInsert |
| `BatchSizeResolverTest` | 500 / 5k / 50k, active source |
| `TargetRowLedgerTest` | единая транзакция ledger + business row, duplicate skip |
| `TargetRowLedgerIntegrationTest` | реальный PostgreSQL: одинаковый local PK, ledger duplicate, rollback/retry |
| `TaskRepositoryTest` | atomic claim и conditional stale release |
| `EtcdTaskFencingIntegrationTest` | реальный etcd: конкурентный claim и zombie Worker после lease loss |
| `BatchProcessorSqlTest` | `[from,to)`, первый запрос без synthetic PK |
| `TargetIndexEnsurerTest` | quoted identifiers и лимит имени индекса 63 символа |
| `PartitionCompletionDetectorTest` | завершение месяца |
| `PartitionNameResolverTest` | `orders_2024_01` |

Integration tests используют `postgres:16-alpine` и
`quay.io/coreos/etcd:v3.5.15`. При недоступном Docker они пропускаются через
`disabledWithoutDocker`; при доступном Docker входят в обычный `mvn test`.

---

## Ключевые инварианты

```text
 1. Только одна активная migration                    ✅
 2. Только Leader управляет migration                 ✅
 3. Worker не создаёт Task самостоятельно             ✅
 4. Один Pod = max 1 Task                             ✅
 5. Pod / Worker — динамически                        ✅
 6. Task создаются динамически (sliding window)       ✅
 7. Все диапазоны заранее в etcd не создаются         ✅
 8. Checkpoint — только Leader, последовательно       ✅
 9. Task ownership: lease + generation + token + CAS    ✅
10. Source PK не включается в INSERT                  ✅
11. Idempotent INSERT через transactional ledger       ✅
12. Verify-индексы только на новых shard              ✅
13. Партиционированные: индексы после залития месяца  ✅
14. Source удаляется только после verify               ❌ DELETE не реализован
15. Target запись без дублей при reclaim               ✅ ledger UNIQUE
16. business key в конфиге таблиц                      ✅
17. Служебная ledger-таблица создаётся на target       ✅ настраивается
```

---

## Открытые вопросы

### §19 — модель UPDATE/DELETE во время migration

| Вариант | Описание |
|---------|----------|
| **1** | Бизнес-приложение не изменяет уже отобранные записи |
| **2** | Отслеживание через `updated_at` / version |
| **3** | CDC / WAL |

### Известные пробелы

| Пробел | Последствие |
|--------|-------------|
| `DELETE source` не реализован | Данные остаются на обоих shard; инвариант 14 не выполняется |
| `VERIFY` — только hook с логом | Нет подтверждения переноса перед удалением |
| Нет общей транзакции target/source | Ledger атомарен с target INSERT, но DELETE source отсутствует |
| Партиции на target не создаются | INSERT в партиционированную таблицу упадёт, если партиции нет |
| `maxTargetTasks` не используется | Target-слоты не ограничиваются |
| Hash key ошибочно объявлен уникальным | Ledger пропустит вторую строку; уникальность — обязательный контракт данных |
| Incremental pipeline после T0 | `isCaughtUp` — заглушка |

### Следующие шаги

1. **COPY через CopyManager** на новые shard (batch 50k) + полный набор колонок
2. **VERIFY** по date-полям и business key (JDBC)
3. **DELETE source** после подтверждённого verify
4. Ограничение fan-out по `maxTargetTasks`
5. **Incremental pipeline** после T0
6. Выбор и реализация модели §19
7. Integration + load tests

---

## Лицензия

Internal project.
# etcd-reshard-2
