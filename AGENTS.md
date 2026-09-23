# AGENTS.md

Instructions for anyone (human or coding agent) changing this repository.

## GitHub workflow (mandatory)

`main` on GitHub accepts changes **only through pull requests**. Direct commits, direct pushes, force-pushes, and merges to `main` are forbidden.

Coding agents **open a pull request and stop**. The user reviews the PR and merges it. Do not merge, squash-merge, rebase-merge, approve, or enable auto-merge unless the user explicitly asked in that conversation.

Required sequence:

1. Fetch and branch from latest `origin/main`. Do not commit on `main`.
2. Put the work on a feature branch (`docs/…`, `feat/…`, `fix/…`).
3. Push **that branch** (`git push -u origin <branch>`). Never `git push origin main`.
4. Open a PR targeting `main`.
5. Hand the PR URL to the user. Do not merge it.

Also forbidden: deleting `main`, force-pushing `main` or the PR branch after review comments unless the user asked to rewrite, and pushing leftover `execute-plan/*` branches unless asked.

The design contract is `docs/architecture.md`. If code and that document disagree, stop and update the document first. Do not silently change Phase 1 invariants, the REST contract, the schema, or the transaction model.

This is a **durable current-state orchestrator** (Conductor / Step Functions / saga orchestrator shape). It is **not** a Temporal clone. Do not introduce history replay, deterministic workflow-as-code, task-queue matching, query handlers, or Temporal type/protocol names.

Honesty labels used in `docs/architecture.md`:

- **Phase 1** — implemented. Single Spring Boot process + PostgreSQL. Linear `ORDER` saga. In-process stub activities. REST start/query. No auto-resume.
- **Planned** — later increment. Do not start unless the user asked for that phase.
- **Theoretical** — vocabulary only. Not committed.

Current code is Phase 5 (PR-08): the engine commits `RUNNING`, publishes `activity.tasks`, and applies `activity.results`. One `worker` module runs the five stubs. Republish keeps the same `attempt`. `FAILED` is not retried. Next planned increment is Phase 6 (activity idempotency) when that phase is requested. Do not scaffold compensation, signals, timer steps, a second worker, or a designer "for later".

---

## Tech stack

| Item | Standard |
|---|---|
| Language | Java 21 (`maven.compiler.release=21`) |
| Build | Maven 3.9+ multi-module aggregator |
| App | Spring Boot **4.1.x** (BOM import; parent is **not** `spring-boot-starter-parent`) |
| Persistence | PostgreSQL 16, Flyway SQL, Spring Data JPA |
| API | REST JSON under `/api/v1` |
| Tests | JUnit Jupiter (managed JUnit 6), AssertJ, Spring Boot Test, Testcontainers 2 PostgreSQL |
| Local infra | `docker-compose.yml` — Postgres, Kafka, engine, and worker |

Do not add Spring Statemachine, Kafka, Redis, Elasticsearch, gRPC, Micrometer dashboards, or extra Compose services unless the matching phase is in progress.

---

## Modules and packages

```
distributed-workflow-engine/     aggregator POM; Boot BOM; pluginManagement
  engine-api/                    JDK-only contracts (no Spring, JPA, Jackson, Kafka, Lombok)
  worker/                        One Spring Boot process. Kafka consumer. Five stub activities.
  engine/                        Spring Boot app + orchestration. Publishes tasks, applies results.
```

Maven coordinates: `groupId` `com.workflowengine`, version `0.1.0-SNAPSHOT`.

### `engine-api` (`com.workflowengine.api`)

Allowed: JDK types only. JSON travels as `String` (UTF-8 JSON text). Status enums, snapshots, `StartWorkflowCommand`, `Activity`, `ActivityContext`, `ActivityResult`.

Forbidden here: `WorkflowDefinition`, `StepDefinition`, JPA entities, Spring types, Jackson, HTTP DTOs that leak servlet/Jackson types.

A worker module depends on `engine-api`, never on `engine`.

### `engine` package map

| Package | Owns |
|---|---|
| `com.workflowengine` | `WorkflowEngineApplication` |
| `com.workflowengine.web` | REST adapters only. Not named `.api`. |
| `com.workflowengine.application` | Admission, get, snapshots, request-thread exceptions |
| `com.workflowengine.domain` / `definition` | Workflow/step definitions and registry |
| `com.workflowengine.worker` | One worker process. Kafka listener and the five stub activities. |
| `com.workflowengine.runtime` | `WorkflowExecutor`, `RecoveryScanner`, `TimeoutPoller` |
| `com.workflowengine.persistence` | Entities, Spring Data, Flyway-aligned mapping |
| `com.workflowengine.config` | Boot configuration beans |

`WorkflowExecutor` must not import `com.workflowengine.worker`. It publishes a task and applies a result. There is a bytecode test that it does not name the stub package; keep it green.

Do not create empty `worker*` modules. Modules appear in the PR that first has a class a JVM will load.

---

## Architecture invariants (Phase 1)

These are load-bearing. A PR that violates them is out of spec even if tests are green by accident.

### Product shape

- PostgreSQL is the source of truth for orchestration metadata. Kafka, when it appears, is transport only.
- The engine owns workflow/step lifecycle. It does **not** own orders, inventory, payments, shipments, or notifications.
- Stubs return canned JSON. They do not check stock or talk to a card network. Do not add business tables to the engine schema.
- No XA / two-phase commit. Ever.
- Spelling is `CANCELED` (one L) when that state is introduced. Do not add enum values until a phase implements the transition.

### Admit-then-run

1. The **admit transaction** commits instance `PENDING` + all steps `PENDING`.
2. HTTP may return `201` + `Location` + **the in-memory admit snapshot**. Do not reload after submit. A fast executor must not change the `201` body.
3. Only the INSERT winner submits `WorkflowExecutor.run(id)` on `workflowTaskExecutor`.
4. Idempotent retry (unique violation on `idempotency_key`) returns the existing snapshot and **must not** submit the executor.
5. There is no `?wait=` flag. Clients and tests poll `GET`.

### Commit-before-invoke

Every state transition is its own committed transaction. `Activity.execute` runs with **no** open workflow transaction.

Do **not** put `@Transactional` on `StartWorkflowService.start`, `WorkflowExecutor.run`, or any method that both writes workflow state and invokes an activity.

Fold instance `PENDING→RUNNING` into the **first step-start transaction**. Do **not** add a separate instance-only `RUNNING` transaction.

Commit last-step `COMPLETED` and instance `COMPLETED` in **one workflow-complete transaction**. A crash window of instance `RUNNING` + all steps `COMPLETED` is forbidden.

### Crash contract (Phase 2)

After a committed `RUNNING` write, invoke. On process death the **recovery scanner** resumes leftovers. Idempotent `200` still must not submit the executor.

| Leftover | Phase 2 |
|---|---|
| Instance `PENDING`, all steps `PENDING` | Resume: start the first step |
| Instance `RUNNING`, one step `RUNNING`, deadline still in the future, `next_attempt_at` due | Republish that task at the same `attempt`. Do not invoke inside the engine. |
| Instance `RUNNING`, `deadline_at` due | Phase 3: step-fail with error `TIMED_OUT`. Do not increment `attempt`. Do not invoke. |
| Instance `FAILED` | Leave terminal. Do not retry. Timeout uses this status; there is no `TIMED_OUT` enum value. |

### Step timeout (Phase 3)

- `StepDefinition.timeout` is a per-attempt limit. Null means no deadline. `ORDER` uses `OrderWorkflowDefinition.STEP_TIMEOUT` (5 minutes).
- Step-start and running-resume set `workflow_step.deadline_at` from the engine `Clock`. `started_at` stays PostgreSQL `now()`.
- `TimeoutPoller` writes the step-fail transaction. It does not submit `WorkflowDispatcher`, because the inflight set would hide an attempt that is still inside `execute`.
- Do not interrupt the activity thread. If it returns after the timeout commit, discard the completion or activity failure. Do not continue to the next step.
- A due deadline wins over running-resume and over `RETRY_EXHAUSTED`.

Same-process double-submit is prevented by `WorkflowDispatcher`'s inflight set. `RetryPolicy.maxAttempts` caps `RUNNING` resumes; exceeding it writes `RETRY_EXHAUSTED` and `FAILED`.

### Optimistic locking

JPA `@Version` on `WorkflowInstanceEntity.version` is the **only** incrementer. Do not write `version = version + 1` in SQL or `@Modifying` queries.

Happy-path terminal `version == 10`. Tests must assert that after `COMPLETED`.

Mid-step complete transactions that do not change instance status still bump `@Version` (use `LockModeType.OPTIMISTIC_FORCE_INCREMENT` when the instance row would otherwise be unchanged).

### Inter-step I/O

- Every activity receives `workflowInputJson` re-serialized from instance `input_json` (same JSON **value** for every step). Not the raw HTTP bytes.
- `stepInputJson` is always `null`. Step `input_json` stays SQL `NULL`.
- Do not chain step N-1 `output_json` into step N.
- On instance `COMPLETED`, copy the last step’s `output_json`. On `FAILED` / in-flight, instance `output_json` is `null`.

### Identity and idempotency

- Server-generated workflow UUID. Not a client business id.
- `idempotencyKey` is required, 1..128 characters. Missing/blank/too long → `400`.
- Unique index `uq_workflow_instance_idempotency_key`. Concurrent POSTs: one INSERT wins and submits once; loser reloads and returns `200`.

### Activity port

The worker's invoker is local to that process. Lookup miss → `ActivityResult(false, null, "UNKNOWN_ACTIVITY:" + stepName)`. `Activity.execute` throwing → `ACTIVITY_EXCEPTION:…`. Neither throws to the listener. The engine does not block on the worker. A producer `get` waits only for the broker ack.

`failAt` is honored only when the worker's `spring.profiles.active=test`.

`failAt` is a demo backdoor on stubs only. Unknown `failAt` → all steps succeed. Future workers ignore it unless `spring.profiles.active=test`.

### Executor-thread vs request-thread failures

| Where | What | Client sees |
|---|---|---|
| Request thread (POST/GET) | Validation, admit transaction, unique-violation reload, GET load | `400` / `404` / `413` / `503` / `500`. Generic body. **No SQL.** |
| Executor / `TaskExecutor` thread | Persistence, `@Version` conflict, unexpected transition failure | Log ERROR with `workflowId`. **Stop.** Do not retry the activity. Do not mark the step `FAILED`. Subsequent `GET` is `200` with the leftover. |

---

## REST surface (Phase 1)

Base path `/api/v1`. JSON. No auth. Bind for local use only.

| Method | Path | Contract |
|---|---|---|
| `POST` | `/api/v1/workflows` | Admit. `201` + `Location: /api/v1/workflows/{id}` + admit body, or `200` existing snapshot |
| `GET` | `/api/v1/workflows/{id}` | Snapshot, steps `ORDER BY position` |

Do not add list, GET-by-key, cancel, signal, or `?wait=` in Phase 1.

`201` body: `status=PENDING`, `version=0`, `currentStep=null`, five `PENDING` steps, `output=null`. Tests must not accept an already-advanced `201`.

Payload limit **65536 bytes** via servlet `Filter` on `/api/` POST/PUT/PATCH. `Content-Length` too large or counted bytes overflowing → `413`. Do not rely on Tomcat multipart/file settings.

Actuator: expose **`health` only**. Do not enable `env`, `heapdump`, `beans`, or `configprops`.

Error bodies are generic (`Bad Request`, `Not Found`, `Payload Too Large`, `Service Unavailable`, `Internal Server Error`). Never include SQLSTATE, SQL text, connection strings, or stack traces.

---

## Persistence

- Flyway SQL under `engine/src/main/resources/db/migration/`. `spring.jpa.hibernate.ddl-auto=none`.
- JSON payloads are `JSONB`. Timestamps are `TIMESTAMPTZ`, assigned by PostgreSQL. Map with Hibernate `@Generated` / `insertable=false, updatable=false`. Tests must not assert equality with `Instant.now()` from the JVM.
- Step order is `workflow_step.position`. Names are not an order. Do not `ORDER BY started_at`.
- Do not add `workflow_event` until a phase that reads it.
- Do not add `CANCELED` / `TIMED_OUT` / `COMPENSATING` status values or check constraints until a phase branches on that status. Phase 3 records timeouts as `FAILED` with error `TIMED_OUT` and column `deadline_at`.
- Schema PRs after Phase 1 are expand/contract if rows exist that matter.

New migrations: `V{n}__{snake_description}.sql`. Never edit an applied `V1__init.sql` once this database has been used beyond a disposable laptop drop.

---

## Java and Spring style

- Match existing code: 4-space indent, K&R braces, no wildcard imports except JUnit/MockMvc static imports in tests.
- **`engine-api` is the only JDK-only module.** Every other module uses Lombok and the other standard libraries in the next section. Do not copy the `engine-api` style into `engine`, `worker`, or a later module.
- JPA entities: `@Getter` `@Setter` `@NoArgsConstructor`. Never `@Data` or `@EqualsAndHashCode` (identity is the UUID). DB-generated timestamps use `@Setter(AccessLevel.NONE)`.
- Logging: `@Slf4j`. Do not declare `LoggerFactory` by hand outside `engine-api`.
- Simple constructor injection: `@RequiredArgsConstructor` on `final` fields. Keep an explicit constructor when it wraps `TransactionTemplate` or validates uniqueness at construction (`StartWorkflowService`, `WorkflowExecutor`, registries).
- Constructor injection only. No field `@Autowired`. No setter injection.
- Prefer `record` for immutable values (commands, snapshots, results, HTTP JSON records). JPA entities stay mutable classes. Lombok does not replace records.
- Jackson 3 (`tools.jackson.*`) in every module except `engine-api`. Annotations stay `com.fasterxml.jackson.annotation` if you add them. Do not import `com.fasterxml.jackson.databind`.

### Standard libraries (every module except `engine-api`)

`engine-api` stays a plain JDK jar: no Lombok, Spring, JPA, Jackson, or Kafka. Every other module uses the libraries this repository already chose. Do not hand-write a replacement.

| Need | Use |
|---|---|
| Getters, setters, constructors, loggers | Lombok: `@Getter`, `@Setter`, `@NoArgsConstructor`, `@RequiredArgsConstructor`, `@Slf4j`. Never `@Data` or `@EqualsAndHashCode`. Do not write those members by hand. |
| JSON | Jackson 3 (`tools.jackson.*`). Do not write a JSON parser or codec. |
| Kafka task and result values | The `engine-api` records themselves. Spring Kafka `JacksonJsonSerializer` and `JacksonJsonDeserializer` (Jackson 3). Do not use the deprecated Jackson 2 `JsonSerializer` / `JsonDeserializer`. Do not build a field map and then turn it into a string. |
| Persistence | Spring Data JPA and Flyway. Do not hand-roll a JDBC client for workflow state. |
| Logging | SLF4J through `@Slf4j`. Do not use `System.out`, `System.err`, or `LoggerFactory`. |
| HTTP | `spring-boot-starter-webmvc` and the existing controller records. Do not add a second web stack. |
| Tests | JUnit Jupiter, AssertJ, Spring Boot test starters, Testcontainers. Do not mock PostgreSQL for state-machine tests. |

A new module follows this table on the day it is created. `engine-api` is the exception, and it stays the exception.

- Web starter is `spring-boot-starter-webmvc` (not `spring-boot-starter-web`). Flyway is `spring-boot-starter-flyway`. Tests use the matching `*-test` starters and Testcontainers 2 artifacts (`testcontainers-postgresql`, `testcontainers-junit-jupiter`).
- `final` on fields that are not reassigned. Do not make entities `final` in a way that breaks Hibernate proxies if proxies appear later; current entities are concrete with no lazy-to-one graphs that require that.
- Java 21 language is fine (records, text blocks, pattern matching for instanceof). Do not use preview features.
- Do not use `System.out` / `System.err`. Use SLF4J.
- Do not swallow exceptions empty. Request-thread mapping and invoker catch-and-convert are explicit, documented contracts.
- Checked exceptions: do not put them on `Activity.execute`. Runtime failures are converted by the invoker.
- Collections: never return `null` lists; use empty lists. Snapshot `steps` are ordered by `position`.
- `Optional` is for repository `find*` misses, not for every field.
- Do not use Spring Statemachine. The state machine is the tables plus `WorkflowExecutor`.
- Keep methods small enough that one unit of work is obvious. A method that opens a workflow transaction must not invoke an activity.
- New Spring `@Bean` / `@Component` types need a reason. Do not add unused configuration.

### Naming

- Types: `UpperCamelCase`. Methods/fields: `lowerCamelCase`. Constants: `UPPER_SNAKE`.
- Packages: `com.workflowengine.{layer}`.
- HTTP records in `web`: `StartWorkflowRequest`, `WorkflowResponse`, `StepResponse`, `ErrorResponse`.
- Domain snapshots in `engine-api`: `WorkflowSnapshot`, `StepSnapshot`.
- Status enums: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`.
- Step names and workflow type `ORDER` are identifiers; do not rename them without a `definition_version` policy.

### Imports and dependencies

- `engine-api/pom.xml` has **no** dependencies beyond the JDK.
- Every other module may use Jackson 3 where it reads or writes JSON, including Kafka values. Spring Kafka serializes `ActivityContext` and `ActivityCompletion`. Persistence still stores JSON as `String` + `jsonb` casts. `engine-api` does not depend on Jackson.
- Do not add a dependency that the matching phase does not need.

---

## Javadoc (mandatory)

Javadoc is **mandatory** for all production and test support types this project keeps. A new person who has not read the PR history must be able to understand a type from its Javadoc plus `docs/architecture.md`.

If you add or edit a type, you write or update its Javadoc in the same change. Do not leave a touched file undocumented. Do not "fix later".

### What must be documented

| Surface | Required |
|---|---|
| Every class, interface, enum, record, annotation | Type-level Javadoc |
| Every enum constant | Constant Javadoc |
| Every public and protected constructor, method, and field | Member Javadoc |
| Record components | `@param` on the record (or compact canonical constructor) |
| Private methods/fields | Javadoc when they encode a transaction boundary, concurrency rule, leftover/crash behavior, or a non-obvious invariant |
| Each package that introduces a concept | `package-info.java` |

Test methods may use a one-line Javadoc or a method name that states the scenario; test **fixtures**, hooks, and support types (`ActivityBlockHook`, `WorkflowAwait`, counters) follow the same type-level rules as production code.

Lombok-generated members (`@Getter`/`@Setter`/`@RequiredArgsConstructor`/`@Slf4j` `log`) do not get handwritten Javadoc. Document the field or the type instead.

### Type-level Javadoc must include

Write complete sentences. Do not restate the class name. Cover all of the following when they apply; if one does not apply, say so briefly (for example "Not thread-safe; request-scoped" or "No special failure handling beyond letting exceptions propagate to `RestExceptionHandler`").

1. **What the type represents or is responsible for.** One or two sentences. Be precise about the boundary (HTTP adapter vs admission vs executor vs persistence vs stub).
2. **Role in the architecture** when that is not obvious from the package. Name the collaborators and the direction of the dependency. Point to the phase (`Phase 1` / `Planned`) when the type is a temporary seam (`InProcessActivityInvoker`, stubs, `failAt`).
3. **Important invariants or lifecycle behavior.** Examples: admit snapshot must remain `PENDING`/`version=0`; `@Version` is the only incrementer; step `position` is order; `output_json` is copied only on instance `COMPLETED`; leftover `RUNNING` is stuck until Phase 2.
4. **Important concurrency or thread-safety characteristics.** Which thread runs it (Tomcat request, `workflow-` executor, later scanner). Whether two threads may touch the same instance. What the unique-key and `@Version` races mean. Whether the type is immutable, request-scoped, or a process-wide singleton.
5. **Important failure or error-handling behavior.** What it returns vs throws. Request-thread HTTP mapping vs executor-thread log-and-stop. Invoker never throws for stub/business failure. Unique-violation reload. Generic error bodies.

### Concepts must be documented where they are introduced

When a type or package introduces a project concept, the Javadoc (type, `package-info.java`, or both) must define it in place. Do not assume the reader already knows these terms. Concepts that already exist and must stay defined at their introduction site:

| Concept | Introduce / keep defined at |
|---|---|
| Admit-then-run | `StartWorkflowService`, `WorkflowController` |
| Commit-before-invoke / unit of work | `WorkflowExecutor` |
| Idempotent admission | `StartWorkflowService`, unique index on `idempotency_key` |
| Leftover (`PENDING` never-started, `RUNNING` mid-step) | `WorkflowExecutor`, Phase 1 crash contract |
| `@Version` / optimistic concurrency | `WorkflowInstanceEntity` |
| Sync in-process `ActivityInvoker` vs later async dispatch | `ActivityInvoker`, `InProcessActivityInvoker` |
| `failAt` demo failure injection | stub package / `StubSupport` |
| Admit snapshot vs polled `GET` | `WorkflowController`, `AdmissionResult` |
| Engine metadata consistency vs business saga eventual consistency | `docs/architecture.md` and engine `package-info` / application types as touched |

When you add a new concept (retry policy, `next_attempt_at`, outbox, compensation, `SKIP LOCKED`, …), define it in Javadoc at the first type that implements it, not only in the design doc.

### Method, constructor, and field Javadoc

- First sentence is a summary that can stand in an index (end with a period).
- `@param` every parameter: meaning, units, nullability, allowed range (for example idempotency key 1..128).
- `@return` what the caller gets, including nullability and admit snapshot vs current snapshot.
- `@throws` every declared or documented unchecked exception the caller must handle (`InvalidStartWorkflowException`, `WorkflowNotFoundException`, `PayloadTooLargeException`).
- Document side effects: which named transaction commits, whether the executor is submitted, whether `attempt` increments, whether a stub is invoked.
- Document thread: "runs on the HTTP request thread" vs "runs on `workflowTaskExecutor`" vs "must be called with no open workflow transaction".
- Use `{@code …}` for types, method names, JSON fields, SQL identifiers, and status values.
- Use `@implNote` for implementation constraints (do not reload after submit; do not catch `Exception` on the executor and convert to step `FAILED`).
- Use `@apiNote` for caller-facing warnings (Phase 1 does not resume; `201` is not terminal).
- Do not write Javadoc that only repeats the identifier (`/** The id. */`). If there is nothing to add, the member is probably in the wrong place or should be documented as part of the type.

### Style

- HTML Javadoc tags: `<p>`, `<ul>/<li>`, `{@code}`, `{@link}`, `{@literal}`.
- `{@link}` to collaborators the reader should open next. Do not `{@link}` every type in the signature.
- Do not paste stack traces, SQL, secrets, or full example payloads with PII.
- Do not put PR numbers or "added in this commit" in Javadoc. Use phase labels.
- Keep Javadoc in sync with behavior. A wrong comment is a bug.
- English, complete sentences, US spelling except `CANCELED` as specified.

### Example shape (illustrative)

```java
/**
 * Persists a new ORDER instance and submits in-process execution after commit.
 *
 * <p>This is the Phase 1 admission service: it is the only writer that creates
 * rows, and the only place that may submit {@link WorkflowExecutor}. HTTP adapters
 * in {@code com.workflowengine.web} call this type; they do not talk to the
 * executor or repositories directly.
 *
 * <p>Invariant: the admit transaction inserts the instance as {@code PENDING}, {@code version = 0},
 * {@code current_step} null, and every step {@code PENDING}. The returned snapshot
 * is that admit aggregate. Callers must not reload it after submit.
 *
 * <p>Concurrency: many request threads may call {@link #start} at once. Concurrent
 * posts with the same idempotency key serialize on
 * {@code uq_workflow_instance_idempotency_key}. Only the INSERT winner submits
 * the executor. This type is a Spring singleton; it holds no per-request state.
 *
 * <p>Failure handling: validation failures throw
 * {@link InvalidStartWorkflowException} (HTTP 400). Unique-key conflict reloads
 * the existing row and returns {@code created = false} without submitting.
 * Executor-thread failures after a successful admit are not reported on this
 * call; clients poll GET.
 */
```

---

## Logging and observability

Structured logs (key=value) on admit, each step transition, complete, and fail.

Required fields: `workflowId`, `type`, `step`, `attempt`, `status`, `version`.

- INFO: lifecycle transitions. Do not log full `input_json` / `output_json` at INFO (PII).
- DEBUG: stub output if needed.
- ERROR: executor-thread infrastructure failures, unexpected request-thread errors.
- WARN: database unreachable on the request thread (`503`).

No Prometheus, Jaeger, ELK, or Zipkin in Compose until Phase 11.

---

## Testing

- JUnit Jupiter. AssertJ. Testcontainers 2 `postgres:16` (`org.testcontainers.postgresql.PostgreSQLContainer`) with `@ServiceConnection` for persistence and API tests.
- No Kafka Testcontainers until Phase 5.
- Do not mock PostgreSQL for state-machine tests. The point is committed visibility.
- Do not use `Instant.now()` equality against DB timestamps.
- Do not `assertEquals` snapshot JSON against the raw POST body (JSONB re-serializes).
- `201` tests must pin the **admit snapshot**: `PENDING`, `version=0`, `currentStep=null`, all steps `PENDING`. Prove it with a blocked first stub while POST returns.
- Poll `GET` (or repository load) until terminal with a ~5s timeout. Happy path: five `COMPLETED` in position order, `attempt = 1`, `version = 10`, `currentStep = SEND_NOTIFICATION`, instance output equals last step output.
- `failAt=PROCESS_PAYMENT` and `failAt=CREATE_ORDER` plus unknown `failAt` are required scenarios when changing the executor or stubs.
- Durability / recovery: block inside the worker `execute`; a **second DB connection** must see `RUNNING`. A **new** engine context against the same database republishes that task at the same `attempt`, and the worker runs it again. Inserting a `RUNNING` row by hand is not a substitute. A `FAILED` instance must not be retried after restart.
- Timeout: move the engine `Clock` (do not `Thread.sleep` for the deadline). An in-flight step past `deadline_at` becomes `FAILED` with error `TIMED_OUT` and a late success must not overwrite it. A restarted process past `deadline_at` must not invoke again.
- Concurrent admit with the same key: one instance, one executor submit. Test the unique-violation path, not only sequential SELECT-then-INSERT.
- Idempotent `200` must not submit a second executor.
- HTTP: unknown type / missing key → `400`; unknown id → `404`; body `> 64 KB` → `413`.
- Keep `WorkflowExecutorPackageTest` (no stub imports) passing.
- Reset stub hooks/counters in `@BeforeEach` / `@AfterEach` when tests share JVM-static stub state.
- New behavior needs a test that would fail if the invariant were broken. Do not add features that cannot be tested locally with Maven + Testcontainers.

Run `mvn test` before considering work done. There is no CI requirement in Phase 1; local green is the gate.

---

## Local run

```text
docker compose up -d --build
# Postgres, Kafka, engine on 127.0.0.1:8080, and the worker
```

Host-side `mvn spring-boot:run` stays available: `docker compose up -d postgres kafka`, then run `engine` and `worker` on the laptop. Those processes use `localhost:5432` and `localhost:9092`. Containers use `postgres:5432` and `kafka:19092`.

Health: `curl -s http://localhost:8080/actuator/health` → `{"status":"UP"}`.

Parent POM skips `spring-boot-maven-plugin` so reactor CLI goals do not run against `engine-api`. `engine` sets `<skip>false</skip>` and `mainClass` `com.workflowengine.WorkflowEngineApplication`. Keep that split.

Do not expose port 8080 past the laptop. Demo idempotency keys should be unguessable if the API is ever networked.

---

## Security

Phase 1 is localhost, no auth, no TLS.

- 64 KB body filter is required (DoS).
- Do not log PII payloads at INFO.
- Do not commit secrets, `.env`, or real credentials. Compose demo password `workflow` is local-only.
- Do not treat `failAt` as a production kill switch.
- Planned before any non-local deploy: authn, separate DB credentials, TLS at the edge, no superuser in committed config.

---

## What not to add

Refuse even if it photographs well. Changing these requires an architecture-doc revision, not a drive-by PR.

- Five microservices, Kafka, gRPC, or empty worker modules on a Phase 1/2 change
- Kafka (or anything else) as source of truth for workflow state
- Shared business tables in the engine database
- XA / 2PC
- Deterministic replay, history service, Temporal SDK types
- Business rules inside the engine or stubs that stop being stubs
- Visual designer, BPMN, custom DSL parser
- Kubernetes operators, service mesh
- Spring Statemachine as the durability story
- One `@Transactional` around admission + all invokes
- Starting the executor on idempotent `200` (scanner resumes leftovers; POST `200` must not submit)
- `replicas > 1` until Phase 5 (out-of-process activities), Phase 6 (idempotent activities), and Phase 9 (claim/lock)
- Optional idempotency keys before a list or lookup-by-key API exists
- Output chaining between steps
- Query handlers (Temporal meaning)

---

## Changing the system

1. Read `docs/architecture.md` for the phase you are in. Phase 1 forks in Key Decisions are decided.
2. If you need a new state, column, endpoint, or module, check the roadmap (Phase 2 recovery, Phase 3 timeout poller, Phase 5 Kafka + one worker, …) and implement that phase — do not invent a parallel design.
3. Update `docs/architecture.md` in the same change when behavior, schema, or API contracts change.
4. Keep README curl path accurate when the local run story changes.
5. Javadoc and tests land with the code.

### Git and pull requests

- **Never commit or push to `main`.** GitHub branch protection requires a pull request. The agent raises the PR; the user reviews and merges.
- Do not merge PRs, even if `gh pr merge` would succeed. Do not use admin bypass.
- Do not commit `target/`, IDE files, or secrets.
- Commit messages: imperative summary line, then a short body that says why. Match existing history (feature commits + `fix: address review feedback for …` when responding to review).
- Do not force-push `main`. Do not force-push a shared PR branch unless the user asked to rewrite it.
- Do not push leftover `execute-plan/*` branches unless asked.

### PR-sized work

Keep changes independently reviewable. Phase 1 was PR-01..PR-04. Later work maps to PR-05+ in `docs/architecture.md`. Do not mix recovery, Kafka, and compensation in one change. One concern per PR.
