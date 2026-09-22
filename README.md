# Distributed Workflow Engine

A Java 21 / Spring Boot 4.1 workflow **orchestrator**. It coordinates a linear e-commerce **ORDER** saga (create order → reserve inventory → process payment → create shipment → send notification) and stores current state in PostgreSQL.

This is **not** Temporal. There is no event-sourced history, no deterministic replay of workflow code, and no task-queue matching. The engine stores *where the saga is now* (`PENDING` / `RUNNING` / `COMPLETED` / `FAILED`) and commits each transition before it calls the next activity.

**Current stage: Phase 5.** The engine and one worker process, plus Postgres and Kafka. The engine commits each step `RUNNING` and publishes a task. The worker runs the canned stub and publishes a result. A lost task is republished at the same attempt. A step past `deadline_at` fails with error `TIMED_OUT`. `FAILED` is not retried. Real side effects still wait for Phase 6 idempotency.

The design contract is [`docs/architecture.md`](docs/architecture.md). Agent rules are in [`AGENTS.md`](AGENTS.md).

---

## Table of contents

1. [Quick start](#quick-start)
2. [What the engine owns (and what it does not)](#what-the-engine-owns-and-what-it-does-not)
3. [Repository layout](#repository-layout)
4. [Concepts](#concepts)
5. [Named transactions](#named-transactions)
6. [Database](#database)
7. [Step-by-step code flow](#step-by-step-code-flow)
8. [Happy-path state table](#happy-path-state-table)
9. [Scenarios](#scenarios)
10. [What changed in Phase 2](#what-changed-in-phase-2)
11. [What changed in Phase 3](#what-changed-in-phase-3)
12. [Current code vs the end goal](#current-code-vs-the-end-goal)
13. [HTTP contract](#http-contract)
14. [Configuration](#configuration)
15. [Tests](#tests)
16. [Roadmap](#roadmap)

---

## Quick start

**Prerequisites:** Java 21, Maven 3.9+, Docker.

```text
docker compose up -d
# wait until postgres is healthy: docker compose ps

mvn -pl engine -am spring-boot:run
# second terminal
mvn -pl worker -am spring-boot:run
```

Health:

```text
curl -s http://localhost:8080/actuator/health
# {"status":"UP"}
```

Interactive API docs (Scalar — cleaner than Swagger UI; try-it-out against this process):

```text
open http://localhost:8080/scalar
# OpenAPI JSON: http://localhost:8080/v3/api-docs
```

Start an ORDER workflow, then poll until it finishes:

```text
curl -sD - -X POST http://localhost:8080/api/v1/workflows \
  -H 'Content-Type: application/json' \
  -d '{"type":"ORDER","idempotencyKey":"order-1001","input":{"customerId":"cust-9","amountCents":4999}}'
# 201 + Location: /api/v1/workflows/<id>
# body is PENDING, version=0 — that is admission, not “saga finished”

curl -s http://localhost:8080/api/v1/workflows/<id>
# poll until status=COMPLETED
```

Compose credentials (local only):

| Setting  | Value    |
|----------|----------|
| Host     | localhost |
| Port     | 5432     |
| Database | workflow |
| User     | workflow |
| Password | workflow |

The engine listens on **8080**. Actuator exposes **`health` only**.

---

## What the engine owns (and what it does not)

The engine owns **orchestration metadata**: workflow id, type, definition version, status, current step, idempotency key, optimistic-lock version, per-step status/attempt/output/error/timestamps.

It does **not** own orders, inventory, payments, shipments, or notifications. The activities are stubs in the `worker` process. They return `{"stub":true,"activity":"CREATE_ORDER"}` (and so on). They do not check stock or talk to a card network. `failAt` works only when that process runs with the `test` profile.

Two consistency domains stay separate:

1. **Engine metadata** — strong consistency. After a commit, `GET` sees that commit.
2. **Business saga** — eventually consistent across real systems (planned, when stubs become workers).

There is no XA / two-phase commit between those domains.

---

## Repository layout

```
distributed-workflow-engine/     Maven aggregator; imports Spring Boot 4.1 BOM
  engine-api/                    JDK-only contracts (no Spring, JPA, Jackson, Lombok)
  engine/                        Spring Boot app: HTTP, admission, executor, scanner, stubs, JPA
  docker-compose.yml             Postgres 16 only
  docs/architecture.md           Design contract
  AGENTS.md                      Rules for humans and coding agents
```

**`engine-api`** (`com.workflowengine.api`): `WorkflowStatus`, `StepStatus`, `StartWorkflowCommand`, `WorkflowSnapshot`, `StepSnapshot`, `Activity`, `ActivityContext`, `ActivityResult`. JSON travels as `String`. A future `worker` module must depend on this jar only.

**`engine` package map:**

| Package | Role |
|---|---|
| `com.workflowengine.web` | REST adapters. Not named `.api`. |
| `com.workflowengine.application` | Admit, get, snapshots. Request-thread exceptions. |
| `com.workflowengine.domain` / `definition` | `ORDER` graph, `RetryPolicy`. |
| `com.workflowengine.worker` | Kafka listener and the five stub activities. |
| `com.workflowengine.runtime` | `WorkflowExecutor`, `WorkflowDispatcher`, `RecoveryScanner`, `TimeoutPoller`. |
| `com.workflowengine.persistence` | JPA entities and Spring Data. |
| `com.workflowengine.config` | Task executor, clock, daemon scheduler. |

`WorkflowExecutor` must not import the worker package. It publishes a task and applies the result.

---

## Concepts

### Admit-then-run

Admission is durable **before** any activity runs. `POST` may return as soon as the instance row is committed. The saga runs on a `workflow-*` thread afterward. Clients and tests wait by polling `GET`. There is no `?wait=` flag.

### Commit-before-invoke

Every state transition is its own committed Postgres transaction. `Activity.execute` runs with **no** open workflow transaction. If the process dies during invoke, the last **committed** leftover is still in the database (`RUNNING` step). That is the point of the durability test.

Do not put `@Transactional` on `StartWorkflowService.start` or `WorkflowExecutor.run`.

### Idempotent admission

`idempotencyKey` is **required** (1–128 characters). Unique index `uq_workflow_instance_idempotency_key`. Concurrent POSTs: one INSERT wins and submits the executor; the loser reloads and returns `200`. The `200` path **must not** submit the executor (Phase 2 recovery is what continues a leftover, not a retry POST).

### Leftover

Committed state left after a crash or a lost submit:

| Leftover | Meaning |
|---|---|
| Instance `PENDING`, all steps `PENDING` | Admit committed; first step-start never ran (or submit was lost). |
| Instance `RUNNING`, one step `RUNNING` | Crash during invoke after the step-start commit. |
| Instance `FAILED` | Terminal. Not retried. A timeout is this status with error `TIMED_OUT`. |

### Optimistic locking

JPA `@Version` on `workflow_instance.version` is the **only** incrementer. Do not write `version = version + 1` in SQL. Happy-path terminal (no crash) is **`version = 10`**.

Mid-step complete transactions do not change instance status, so they use `LockModeType.OPTIMISTIC_FORCE_INCREMENT` still to bump version.

### Inter-step I/O

Every activity receives the same `workflowInputJson` (JSONB re-serialized from instance `input_json`). `stepInputJson` is always `null`. The engine does **not** chain step N-1 output into step N. On instance `COMPLETED`, instance `output_json` is a copy of the last step’s output. Otherwise it is `null`.

### `failAt`

Demo failure injection on stubs only. If input JSON contains `"failAt":"PROCESS_PAYMENT"`, that stub returns `STUB_FORCED_FAILURE`. Unknown values are ignored; all steps succeed. The invoker does not interpret `failAt`.

### At-least-once (Phase 2)

Re-invoking a leftover `RUNNING` step can run the stub a second time. Stubs are not idempotent. Real side effects need Phase 6 (activity idempotency). Phase 2 makes **progress** after crash; it does not make side effects exactly-once.

### Step timeout (Phase 3)

Each ORDER step has a 5-minute per-attempt deadline (`OrderWorkflowDefinition.STEP_TIMEOUT`). Step-start and running-resume store it on `workflow_step.deadline_at` using the engine `Clock`, not PostgreSQL `now()`.

`TimeoutPoller` loads `RUNNING` instances on startup and every `workflow.timeout.interval-ms`. A due deadline commits step and instance `FAILED` with error `TIMED_OUT`. `attempt` does not increase. The activity is not called again. Later steps stay `PENDING`.

The poller does not submit through `WorkflowDispatcher`. That inflight set would skip a workflow whose stub is still inside `execute`. The stub is not interrupted. If it returns success afterward, the completion is discarded and the walk stops.

A crash leftover whose deadline is already due fails the same way. Timeout wins over another attempt and over `RETRY_EXHAUSTED`. `FAILED` stays terminal. There is no `TIMED_OUT` status value.

---

## Named transactions

Do not number them TX1/TX2. Names describe the unit of work:

| Name | Writes | Then |
|---|---|---|
| **Admit** | Instance `PENDING`, `version=0`, five steps `PENDING`, `attempt=0` | HTTP may return `201`. Only the INSERT winner submits the executor. |
| **First step-start** | First step `RUNNING`, `attempt=1`, `started_at=now()`, `deadline_at` from the engine clock; instance `RUNNING`, `current_step=CREATE_ORDER`; `version` 0→1 | **Commit. Then** invoke. |
| **Step-start** | Later step `RUNNING`, `attempt++`, `started_at=now()`, `deadline_at` refreshed; instance `current_step` updated; `version` +1 | **Commit. Then** invoke. |
| **Mid-step complete** | Step `COMPLETED`, `output_json`, `completed_at=now()`; instance status unchanged; `version` +1 | Next step-start. |
| **Workflow-complete** | Last step `COMPLETED` **and** instance `COMPLETED` in **one** commit; instance `output_json` = last step output | Stop. |
| **Step-fail** | Step and instance `FAILED` in one commit; instance `output_json=null`; later steps stay `PENDING`. Timeout uses this transaction with error `TIMED_OUT` and does not increment `attempt`. | Stop. A write is skipped when the step is no longer `RUNNING`. |
| **Running-resume** (Phase 2) | Leftover `RUNNING` step still inside `deadline_at`: `attempt++`, refresh `deadline_at`, `@Version` bump. If `deadline_at` is due, this transaction is a step-fail (`TIMED_OUT`) instead. | **Commit. Then** invoke again, unless the deadline was due. |

Fold instance `PENDING→RUNNING` into first step-start. Do not add a separate instance-only `RUNNING` transaction.

Last-step complete and instance complete share one transaction so you never get instance `RUNNING` + all steps `COMPLETED`.

---

## Database

Flyway only. `spring.jpa.hibernate.ddl-auto=none`. Migrations:

- `V1__init.sql` — instance + step tables, unique keys, status index, `updated_at` trigger.
- `V2__step_next_attempt_at.sql` — Phase 2: `workflow_step.next_attempt_at` and a partial index.
- `V3__step_deadline_at.sql` — Phase 3: `workflow_step.deadline_at` and a partial index.

JSON is `JSONB`. `started_at`, `completed_at`, `created_at`, and `updated_at` are `TIMESTAMPTZ` assigned by PostgreSQL (`now()`). `next_attempt_at` and `deadline_at` are written from the engine `Clock`. Tests must not assert equality with `Instant.now()` from the JVM.

### `workflow_instance`

| Column | Role |
|---|---|
| `id` | Server-generated UUID. Not a client business id. |
| `type` | Definition type (`ORDER`). |
| `definition_version` | Copied from `OrderWorkflowDefinition.version()` at admit (currently 1). |
| `status` | `PENDING` / `RUNNING` / `COMPLETED` / `FAILED`. |
| `input_json` | Opaque workflow input. |
| `output_json` | Last-step output on `COMPLETED`; otherwise `NULL`. |
| `current_step` | `NULL` if never started; running/last/failed step name otherwise. |
| `idempotency_key` | Required, unique, 1–128 chars. |
| `version` | JPA `@Version`. Happy path ends at 10. |
| `error` | Copy of failed step error. |
| `created_at` / `updated_at` | DB clock. `updated_at` is a `BEFORE UPDATE` trigger. |

Indexes: unique `idempotency_key`; `idx_workflow_instance_status` for the Phase 2 scanner (`WHERE status IN ('PENDING','RUNNING')`).

### `workflow_step`

| Column | Role |
|---|---|
| `id` | UUID. |
| `workflow_instance_id` | FK to instance. |
| `name` | Activity name (`CREATE_ORDER`, …). Unique per instance. |
| `position` | Zero-based definition order. Unique per instance. **This** is order, not name, not `started_at`. |
| `status` | `PENDING` / `RUNNING` / `COMPLETED` / `FAILED`. |
| `attempt` | 0 while `PENDING`; incremented on each step-start **and** each Phase 2 running-resume. |
| `input_json` | Always `NULL` in Phase 1/2. |
| `output_json` | Stub/activity output. |
| `error` | Set on `FAILED`. |
| `started_at` / `completed_at` | DB `now()` at start / complete-or-fail. |
| `next_attempt_at` | Phase 2. When a leftover `RUNNING` step is due. `NULL` means due now. `FAILED` ignores this. |
| `deadline_at` | Phase 3. Engine-clock end of the current attempt. `NULL` means no timeout. Due + `RUNNING` → `FAILED` / `TIMED_OUT`. Refreshed on running-resume. |

### How rows change (happy path, no crash)

Admit (one commit):

```
instance: PENDING, version=0, current_step=NULL, output_json=NULL
steps 0..4: PENDING, attempt=0, timestamps NULL
```

After first step-start, then invoke, then mid-step complete, … through workflow-complete:

See [Happy-path state table](#happy-path-state-table). Each line is one committed transaction touching the instance row (`version` +1).

### How rows change (Phase 2 resume of `RUNNING`)

Crash after first step-start (`version=1`, step 0 `RUNNING`, `attempt=1`). Scanner submits. Executor **running-resume**: `attempt` 1→2, `version` 1→2, then invoke, then mid-step complete (`version=3`). Later steps follow the usual table. Terminal version is **greater than 10**.

---

## Step-by-step code flow

### A. `POST /api/v1/workflows` (request thread)

1. `BodySizeFilter` (highest precedence) rejects bodies over **65536** bytes with 413.
2. `WorkflowController.start` maps JSON to `StartWorkflowCommand` (`type`, `idempotencyKey`, `input` serialized with Jackson 3 to a `String`).
3. `StartWorkflowService.start`:
   - Validates type (`ORDER` only), key 1..128, non-blank input JSON. Failures → `InvalidStartWorkflowException` → HTTP 400, generic body.
   - **Admit transaction:** insert instance `PENDING` + five `PENDING` steps (`saveAndFlush`). Unique violation on `idempotency_key` → reload by key → `AdmissionResult(created=false)` → HTTP **200**, **no submit**.
   - INSERT winner: `WorkflowDispatcher.submit(id)` then return `AdmissionResult(created=true)` with the **in-memory admit snapshot**. Do **not** reload after submit. HTTP **201** + `Location: /api/v1/workflows/{id}`. Body is always `PENDING` / `version=0` / `currentStep=null` / five `PENDING` steps.
4. The request thread never calls `Activity.execute`.

`WorkflowDispatcher` puts the id in an in-memory inflight set and runs `WorkflowExecutor.run` on `workflowTaskExecutor` (`workflow-*` daemon threads). If the id is already inflight, submit is a no-op (scanner vs admission race).

### B. Executor walk (workflow thread)

`WorkflowExecutor.execute`:

1. Load type, `definition_version`, and `input_json` once (**seed**). Every activity gets that same input.
2. Resolve `OrderWorkflowDefinition` (five `StepDefinition`s, default `RetryPolicy` maxAttempts=3, backoff=0, timeout 5 minutes).
3. One step per `run`:
   - Instance `COMPLETED` / `FAILED`, or a step already `FAILED` → stop.
   - Next `PENDING` step → **step-start** (`RUNNING`, `attempt++`, `started_at=now()`, `deadline_at`, `next_attempt_at` = now + republish delay), then **publish** `activity.tasks`. The engine returns. It waits for the broker ack, not for the worker.
   - `RUNNING` and `deadline_at` due → **step-fail** `TIMED_OUT`. Do not publish.
   - `RUNNING` and `next_attempt_at` still in the future → stop.
   - `RUNNING` and due for republish → publish the same `attempt` again. Do not increment it.
4. The worker runs the stub. Lookup miss → result `UNKNOWN_ACTIVITY`. Thrown exception → `ACTIVITY_EXCEPTION`. `failAt` applies only in the worker's test profile.
5. The engine consumes `activity.results`:
   - Step no longer `RUNNING`, or the attempt does not match → skip the write.
   - `success=false` → **step-fail**, stop.
   - Last step success → **workflow-complete** (step + instance `COMPLETED` together).
   - Otherwise → **mid-step complete**, then `run` again to start the next step.

Infrastructure failures on `run` are logged and stop. They do **not** mark the step `FAILED`. A result-listener failure other than an optimistic-lock loss is retried by Kafka. `GET` still returns 200 with the leftover. The scanner republishes a due `RUNNING` step that is still inside its deadline. The timeout poller fails one that is not.

### C. `GET /api/v1/workflows/{id}` (request thread)

`GetWorkflowService` loads the instance with steps (`ORDER BY position`). Missing id → 404. Leftovers (`PENDING` / `RUNNING` / `FAILED`) are returned as-is. Executor-thread failures are never HTTP 500 on GET.

### D. Recovery scanner (Phase 2)

`RecoveryScanner`:

- Runs once on `ApplicationReadyEvent` and every `workflow.recovery.interval-ms` (default 2000).
- `SELECT` instances with status `PENDING` or `RUNNING` (`idx_workflow_instance_status`).
- Due **PENDING**: all steps still `PENDING` → `dispatcher.submit`.
- Due **RUNNING**: a `RUNNING` step with `next_attempt_at` null or ≤ now → `dispatcher.submit`.
- Never selects `FAILED` or `COMPLETED`.

Same-process duplicates: dispatcher inflight set. After a process crash the set is empty, so leftovers submit again. A due `deadline_at` is not submitted; the timeout poller fails that step. If a submit is already queued and the deadline elapses before resume, the executor writes `TIMED_OUT` instead of invoking.

### E. Timeout poller (Phase 3)

`TimeoutPoller`:

- Runs once on `ApplicationReadyEvent` and every `workflow.timeout.interval-ms` (default 2000). The interval is the poll period, not the step timeout.
- Loads `RUNNING` instances. A `RUNNING` step with `deadline_at <=` the engine clock is failed in place (`FAILED`, error `TIMED_OUT`).
- Does not call `WorkflowDispatcher`, so an in-flight `execute` cannot hide the row. The stub keeps running until it returns; the later complete or fail write is skipped.
- `workflow.timeout.enabled=false` stops this scan only. Running-resume still fails an already-due leftover.

---

## Happy-path state table

`N` is instance `version` **after** the commit. No crash.

| Event | Instance | Step that changed | `version` | `current_step` |
|---|---|---|---|---|
| Admit | `PENDING` | five × `PENDING`, `attempt=0` | 0 | `NULL` |
| Start step 1 | `RUNNING` | `CREATE_ORDER` `RUNNING` `attempt=1` | 1 | `CREATE_ORDER` |
| Complete step 1 | `RUNNING` | `CREATE_ORDER` `COMPLETED` | 2 | `CREATE_ORDER` |
| Start step 2 | `RUNNING` | `RESERVE_INVENTORY` `RUNNING` | 3 | `RESERVE_INVENTORY` |
| Complete step 2 | `RUNNING` | `RESERVE_INVENTORY` `COMPLETED` | 4 | `RESERVE_INVENTORY` |
| Start step 3 | `RUNNING` | `PROCESS_PAYMENT` `RUNNING` | 5 | `PROCESS_PAYMENT` |
| Complete step 3 | `RUNNING` | `PROCESS_PAYMENT` `COMPLETED` | 6 | `PROCESS_PAYMENT` |
| Start step 4 | `RUNNING` | `CREATE_SHIPMENT` `RUNNING` | 7 | `CREATE_SHIPMENT` |
| Complete step 4 | `RUNNING` | `CREATE_SHIPMENT` `COMPLETED` | 8 | `CREATE_SHIPMENT` |
| Start step 5 | `RUNNING` | `SEND_NOTIFICATION` `RUNNING` | 9 | `SEND_NOTIFICATION` |
| Complete step 5 | **`COMPLETED`** | `SEND_NOTIFICATION` `COMPLETED`; instance `output_json` = last output | **10** | `SEND_NOTIFICATION` |

`failAt=PROCESS_PAYMENT`: versions 0..5, then step-fail → instance `FAILED`, `version=6`, `current_step=PROCESS_PAYMENT`. Steps 0–1 `COMPLETED`, step 2 `FAILED`, steps 3–4 `PENDING`.

---

## Scenarios

### 1. Happy path

`POST` → 201 admit snapshot → poll `GET` → `COMPLETED`, `version=10`, five `COMPLETED` steps, `attempt=1` each, instance output equals last step output.

### 2. Idempotent retry

Same `idempotencyKey` again → 200, same id, no second executor. If the first run already finished, stub counters do not increase.

### 3. Forced failure

`"failAt":"PROCESS_PAYMENT"` → `FAILED` at payment. `"failAt":"CREATE_ORDER"` → first step fails, rest `PENDING`. Unknown `failAt` → all succeed.

### 4. Validation / limits

Unknown `type` or missing key → 400. Unknown id → 404. Body > 64 KB → 413. Error JSON is generic (`Bad Request`, …). No SQL in the body.

### 5. Crash after admit, before first step-start (Phase 2)

Leftover: instance `PENDING`, all steps `PENDING`. Client retries POST → 200 + id, **does not** submit. On process start, scanner submits. Saga runs as a normal first start (`attempt=1`, terminal `version=10`).

### 6. Crash during invoke (Phase 2)

Leftover: instance `RUNNING`, one step `RUNNING`, `attempt=1`. Restart → scanner submits → running-resume `attempt=2` → invoke again → complete. First step ends with `attempt=2`. Terminal `version` > 10.

A second JDBC connection can see `RUNNING` **while** `execute` is still blocked (commit-before-invoke). That is required; inserting a `RUNNING` row by hand is not a substitute for the automated test.

### 7. Failed workflow after restart (Phase 2)

`FAILED` stays `FAILED`. Scanner does not select it. Stub invocation count does not increase.

### 8. Poison / retry cap (Phase 2)

Default `maxAttempts=3`. First start uses attempt 1. Each running-resume increments. If the next attempt would be 4, the executor writes `RETRY_EXHAUSTED` and **step-fail**. That `FAILED` is then left alone.

### 9. Executor-thread infrastructure failure

Persistence or `@Version` conflict on the `workflow-*` thread: log ERROR, stop, do not mark `FAILED`. GET is 200 with leftover. Scanner may submit again if the deadline is still in the future.

### 10. Step timeout (Phase 3)

`CREATE_ORDER` is blocked inside `execute` (`RUNNING`, `attempt=1`, `version=1`, `deadline_at` set). A poller pass before the deadline changes nothing. Move the engine clock past `deadline_at` and scan again: instance `FAILED`, error `TIMED_OUT`, attempt stays 1, later steps `PENDING`, `version=2`. Release the stub. The late success does not overwrite `FAILED` and does not start `RESERVE_INVENTORY`.

Restart after the deadline has passed: the new process does not invoke the stub again. `attempt` stays 1.

---

## What changed in Phase 2

Phase 1 left leftovers stuck on purpose: restart did **not** resume, and idempotent `200` did not start the executor. Phase 2 keeps the second rule and **adds a scanner** so leftovers make progress.

| Area | Phase 1 | Phase 2 |
|---|---|---|
| Never-started `PENDING` | Stuck | Scanner starts the first step |
| Leftover `RUNNING` | Stuck; no second invoke | Re-invoke, `attempt++` |
| `FAILED` | Terminal | Still terminal (not retried) |
| Idempotent POST `200` | Must not submit | Unchanged — scanner continues leftovers |
| Schema | `V1` only | `V2`: `workflow_step.next_attempt_at` |
| Definition | Step name only | `RetryPolicy` (max attempts, backoff) |
| Submit path | Admission called `TaskExecutor` + executor directly | `WorkflowDispatcher` (inflight set) used by admission **and** scanner |
| Executor walk | `startStep` only; non-`PENDING` stopped the whole run | `prepareStep`: skip `COMPLETED`, resume `RUNNING`, stop if not due / poison |
| Startup | No scan | `RecoveryScanner` on `ApplicationReadyEvent` + every 2s |
| Durability test | New context must **not** increment stub counter | New context **must** re-invoke (`attempt=2`) then complete |
| Tests | 24 | 26 (`WorkflowRecoveryTest`: pending resume + failed-not-retried) |

---

## What changed in Phase 3

Phase 2 would re-invoke a `RUNNING` step whenever `next_attempt_at` was due, with no limit on how long the attempt could sit there. Phase 3 adds that limit.

| Area | Phase 2 | Phase 3 |
|---|---|---|
| How long a step may stay `RUNNING` | Unbounded | `deadline_at` = engine clock + `StepDefinition.timeout` (ORDER: 5 minutes) |
| Deadline elapsed while `execute` is in progress | Invoke finishes and may complete the step | Poller commits `FAILED` / `TIMED_OUT`. Late success is discarded. |
| Deadline elapsed across a crash | Next process re-invokes (`attempt++`) | Next process fails the step. No second invoke. `attempt` stays. |
| Status value | `FAILED` for activity failure and `RETRY_EXHAUSTED` | Same `FAILED`. Timeout is the error text `TIMED_OUT`, not a new enum. |
| Schema | `V2` `next_attempt_at` | `V3` `deadline_at` |
| Startup | Recovery scan | Recovery scan and timeout scan |

A null `deadline_at` never times out. Rows that were already `RUNNING` when `V3` was added stay on the Phase 2 resume path until something refreshes the deadline.

**New types:** `RetryPolicy`, `WorkflowDispatcher`, `RecoveryScanner`. **New config:** `workflow.recovery.enabled` (default true), `workflow.recovery.interval-ms` (default 2000). **Clock** bean for due-time. Daemon `TaskScheduler` so recovery ticks do not keep the JVM alive after shutdown.

**Unchanged on purpose:** REST paths, admit snapshot (`201` still `PENDING`/`version=0`), no Kafka, no worker module, no compensation, stubs still canned JSON. No `TIMED_OUT` status value.

---

## Current code vs the end goal

The **end goal** is still this product: a **durable current-state orchestrator** (Conductor / Step Functions / saga shape), not a Temporal clone. History replay, deterministic workflow-as-code, query handlers, and multi-language SDKs stay **theoretical**. What grows is *where* work runs, *how* it is dispatched, and *how* crashes and failures are handled.

Postgres remains the source of truth for orchestration metadata. Kafka, when it appears, is **transport only**.

### Topology

| Concern | Now (Phase 5) | Later |
|---|---|---|
| Processes | Engine JVM + one worker JVM | Optional split into service-owned workers (Phase 8). |
| Compose | Postgres + Kafka | No five microservices. |
| Who runs activities | Worker process implements `Activity` from `engine-api` | Still no card network or stock DB until Phase 6. |
| Engine replicas | `replicas > 1` is a defect | Allowed only after out-of-process activities (5), idempotent activities (6), and `SKIP LOCKED` claim (9). |

### How a step runs

**Now (Phase 5):** commit-before-invoke is **dispatch-and-complete-later**.

1. Engine commits step `RUNNING`.
2. Engine publishes a task (persist-then-publish; no outbox).
3. Engine thread returns. It does not wait for the worker.
4. Worker executes the activity.
5. Worker publishes a result keyed by `(workflowId, step, attempt)`.
6. Engine result consumer applies the mid-step complete, workflow-complete, or step-fail transaction.

**Later:** an outbox can replace the direct publish. Phase 6 makes the worker idempotent on that same key. The producer waits for the broker ack only. It does not wait for the activity result.

### Recovery

| Now | End goal |
|---|---|
| Scanner submits the executor, which republishes a due `RUNNING` task at the same attempt. Lost messages are recovered from the committed row. | Phase 9: multi-instance engines claim rows with `SKIP LOCKED` before publishing. |
| Inflight set is in-memory (one process) | Multi-instance engines claim rows with `SELECT … FOR UPDATE SKIP LOCKED` (Phase 9). `@Version` rejects stale writers. |
| `FAILED` is never retried, including error `TIMED_OUT` | Retry policy on `FAILED` can be turned on once backoff/`next_attempt_at` are used for that path. A distinct `TIMED_OUT` status waits until a phase branches on it. |

### Time, failure, and side effects

| Concern | Now | End goal |
|---|---|---|
| Time | Per-attempt `deadline_at` (ORDER: 5 minutes). Poller fails a due `RUNNING` step with `TIMED_OUT`. Backoff field exists; ORDER uses zero delay. | Phase 10: first-class timer *steps*. The poller is not that. |
| Activity failure | Stub `failAt` or `success=false` → step-fail; instance `FAILED`; later steps stay `PENDING`. | Same forward fail, then **compensation** (Phase 7): reverse walk of completed steps, states `COMPENSATING` / `COMPENSATED`. Requires Phase 6. |
| Double invoke after crash | Real: stub runs twice. Documented. Stubs have no money/stock. | Phase 6: worker idempotency store “this attempt already completed.” Effectively-once = at-least-once delivery + idempotent handler. **Hard gate** before real payment/inventory. |
| `failAt` | Worker honors it only when `spring.profiles.active=test`. | Unchanged. |

### API and definitions

| Now | End goal |
|---|---|
| `POST` / `GET` one workflow. No list, cancel, signal, `?wait=`. | Same admit-then-run. Later: signals `POST /workflows/{id}/signals/{name}`, maybe list/cancel. Still no Temporal query handlers. |
| Linear `ORDER` in Java (`OrderWorkflowDefinition`). | Still Java definitions unless a later phase chooses otherwise. Branching, parallel+join, timer steps (Phase 10). Not BPMN, not a designer. |
| JSON as `String` in `engine-api`; Jackson only at HTTP. | Unchanged contract so workers never depend on `engine`. |

### Data that stays vs data that appears later

**Stays:** `workflow_instance` / `workflow_step` as current state. `@Version`. `definition_version`. `attempt`. `idx_workflow_instance_status`.

**Already added:** `next_attempt_at`, `deadline_at`, `RetryPolicy` and `timeout` on `StepDefinition`.

**Planned tables/states, not present now:** `workflow_event` (audit/UI), outbox (if persist-then-publish drops publishes), `COMPENSATING` / `COMPENSATED` / `CANCELED` / a distinct `TIMED_OUT` status, worker-side idempotency keys.

### Picture

```
Now (Phase 5)
  Client → Engine HTTP → admit commit → workflow-* thread → publish activity.tasks
                         → worker → stub.execute → activity.results → engine complete/fail commit
                         ↘ deadline_at due → TimeoutPoller → FAILED / TIMED_OUT (late result discarded)
                         ↘ lost task, deadline still open → scanner republishes the same attempt
                         ↘ deadline already due → FAILED / TIMED_OUT, no republish

End goal (Phase 5–9, still current-state)
  Client → Engine HTTP → admit commit
       → Engine: RUNNING commit → Kafka task
       → Worker: Activity.execute (idempotent from Phase 6)
       → Kafka result → Engine: complete/fail commit
       → crash leftover → scanner republishes RUNNING to Kafka (not in-process invoke)
       → N engines: SKIP LOCKED claim; workers scaled independently
```

If a sentence in this README and [`docs/architecture.md`](docs/architecture.md) disagree, the architecture doc wins.

---

## HTTP contract

Base path `/api/v1`. JSON. No auth (localhost).

| Method | Path | Result |
|---|---|---|
| `POST` | `/api/v1/workflows` | Admit. `201` + Location + admit body, or `200` existing snapshot |
| `GET` | `/api/v1/workflows/{id}` | Current snapshot, steps by `position` |
| `GET` | `/scalar` | Interactive Scalar API reference (try-it-out) |
| `GET` | `/v3/api-docs` | OpenAPI 3 JSON |

No list, cancel, signal, GET-by-key, or `?wait=` in this phase.

Request:

```json
{
  "type": "ORDER",
  "idempotencyKey": "order-1001",
  "input": { "customerId": "cust-9", "amountCents": 4999 }
}
```

`201` body: `status=PENDING`, `version=0`, `currentStep=null`, five `PENDING` steps, `output=null`. That is **not** terminal.

Statuses: `400` validation, `404` unknown id, `413` payload too large, `503` database unreachable on the **request** thread, `500` unexpected request-thread failure (generic body, no SQL). Executor-thread failures are not HTTP 500.

---

## Configuration

`engine/src/main/resources/application.yml`:

```yaml
server.port: 8080
spring.datasource: jdbc:postgresql://localhost:5432/workflow
spring.jpa.hibernate.ddl-auto: none
spring.flyway.enabled: true
management.endpoints.web.exposure.include: health
workflow.recovery.enabled: true
workflow.recovery.interval-ms: 2000
workflow.timeout.enabled: true
workflow.timeout.interval-ms: 2000
```

Set `workflow.recovery.enabled=false` only in tests that plant leftovers before a second process should resume them. `workflow.timeout.enabled=false` stops the poller only; a running-resume of an already-due step still writes `TIMED_OUT`.

---

## Tests

```text
# Rancher Desktop
export DOCKER_HOST=unix://$HOME/.rd/docker.sock
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock

mvn test
```

Testcontainers starts its own `postgres:16`. You do not need Compose for tests.

| Class | What it proves |
|---|---|
| `WorkflowApiTest` | HTTP admit snapshot, poll to terminal, idempotency, `failAt`, 400/404/413, Scalar UI |
| `WorkflowExecutionTest` | Version 10, `failAt`, concurrent admit, blocked-stub admit snapshot |
| `WorkflowDurabilityTest` | Second connection sees `RUNNING`; new engine republishes the same attempt |
| `WorkflowRecoveryTest` | Never-started `PENDING` completes; `FAILED` not retried |
| `WorkflowTimeoutTest` | Due deadline → `FAILED` / `TIMED_OUT`; late success discarded; restart does not re-invoke |
| `WorkflowPersistenceTest` | Flyway v2, unique keys, `@Version` starts at 0 |
| `BodySizeFilterTest` | 64 KB cap without Spring |
| `WorkflowExecutorPackageTest` | Executor bytecode does not name the stub package |

Manual leftover checks (SQL plant + restart) are described in the Phase 2 PR discussion; automated tests are the gate.

---

## Roadmap

| Phase | Status | Focus |
|---|---|---|
| 1 | Done | Admit-then-run, stubs, REST, commit-before-invoke, no resume |
| 2 | Done | Scanner, `RUNNING` re-invoke, `FAILED` left alone, retry policy + `next_attempt_at` |
| 3 | Done | Timeout poller: `deadline_at`, `FAILED` / `TIMED_OUT`, late completion discarded |
| 4 | Skipped | The worker contract was already `Activity` in `engine-api` |
| 5 | Done | Kafka plus one worker. Republish keeps the same attempt. |
| 6 | Next | Activity idempotency (required before real side effects) |
| 7+ | Planned | Compensation, optional worker split, multi-instance engine, signals, observability |

Do not run `replicas > 1` until out-of-process activities (5), idempotent activities (6), and claim/lock (9) exist.
