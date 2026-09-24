# Engine code flow

The `engine` module is the Spring Boot process that admits an ORDER workflow and moves it forward one committed Postgres transaction at a time. This page follows a start request through the classes that handle it.

Keep this file in the same change as the code it draws. When a class in the table below changes its thread, transaction, status write, publish, or the next class it calls, update the matching diagram and the table row. That includes the worker classes drawn on the result path (`TaskListener`, `ActivityIdempotencyStore`, `WorkerActivityInvoker`). `AGENTS.md` requires the update. A diagram that no longer matches the code is a bug.

A client posts to `POST /api/v1/workflows`. The request thread commits the instance as `PENDING` and returns. A `workflow-*` thread then starts one step and publishes `ActivityContext` on `activity.tasks`. The worker runs the activity. The engine applies `ActivityCompletion` from `activity.results` and either starts the next step, compensates, or stops.

Postgres is the source of truth. Each transition commits before the next publish. The happy path ends at instance `COMPLETED` with `version` 10. `GET /api/v1/workflows/{id}` is how a client sees that progress. The `201` body is the admit snapshot and stays `PENDING` / `version` 0.

## Admit the request

`BodySizeFilter` runs first on the Tomcat thread. `WorkflowController` turns the JSON body into a `StartWorkflowCommand`. `StartWorkflowService` validates it, inserts the rows, and only the insert winner calls `WorkflowDispatcher`.

```mermaid
flowchart TD
  post["Client<br/>POST /api/v1/workflows"]
  filter["BodySizeFilter.doFilterInternal<br/>web/BodySizeFilter.java<br/>POST, PUT, and PATCH under /api/<br/>Content-Length or counted bytes over 65536 returns 413"]
  ctrl["WorkflowController.start<br/>web/WorkflowController.java<br/>toCommand requires an object input<br/>Jackson 3 writes input to a JSON string<br/>Builds StartWorkflowCommand"]
  validate["StartWorkflowService.validate<br/>application/StartWorkflowService.java<br/>WorkflowDefinitionRegistry.findByType<br/>domain/WorkflowDefinitionRegistry.java<br/>ORDER only, via OrderWorkflowDefinition<br/>definition/OrderWorkflowDefinition.java<br/>idempotencyKey length 1..128<br/>inputJson required"]
  bad["RestExceptionHandler.badRequest<br/>web/RestExceptionHandler.java<br/>InvalidStartWorkflowException becomes 400<br/>Database unreachable becomes 503<br/>Generic body, no SQL"]
  admit["StartWorkflowService.insert<br/>application/StartWorkflowService.java<br/>Admit transaction<br/>New UUID, definition_version 1<br/>Instance PENDING, version 0, current_step null<br/>Five steps PENDING, attempt 0, position 0..4<br/>WorkflowInstanceRepository.saveAndFlush<br/>persistence/WorkflowInstanceRepository.java"]
  race{"StartWorkflowService.start<br/>application/StartWorkflowService.java<br/>DataIntegrityViolationException<br/>on uq_workflow_instance_idempotency_key"}
  existing["StartWorkflowService.start<br/>application/StartWorkflowService.java<br/>WorkflowInstanceRepository.findByIdempotencyKey<br/>AdmissionResult created false<br/>application/AdmissionResult.java<br/>WorkflowController returns 200<br/>WorkflowResponses.from<br/>web/WorkflowResponses.java<br/>Dispatcher is not called"]
  submit["WorkflowDispatcher.submit<br/>runtime/WorkflowDispatcher.java<br/>Inflight set rejects a second submit<br/>WorkflowExecutionConfig.workflowTaskExecutor<br/>config/WorkflowExecutionConfig.java<br/>Threads are named workflow-"]
  created["WorkflowController.start<br/>web/WorkflowController.java<br/>HTTP 201 and Location /api/v1/workflows/id<br/>WorkflowResponses.from of the admit snapshot<br/>Still PENDING, version 0"]
  run["WorkflowExecutor.run<br/>runtime/WorkflowExecutor.java"]

  post --> filter --> ctrl --> validate
  validate -->|"invalid type, key, or input"| bad
  validate --> admit --> race
  race -->|duplicate key| existing
  race -->|"insert wins"| submit --> created
  submit --> run
```

`OrderWorkflowDefinition` supplies the five forward steps and their compensators: `CREATE_ORDER`, `RESERVE_INVENTORY`, `PROCESS_PAYMENT`, `CREATE_SHIPMENT`, `SEND_NOTIFICATION`, each with a `COMPENSATE_*` activity and a 5-minute `STEP_TIMEOUT`. Admission copies only the forward names. Compensation rows appear later, after a forward failure.

`RestExceptionHandler` also maps an unknown id to `404` and a body over 64 KB that surfaces during JSON parse to `413`.

## Publish one task

`WorkflowExecutor.run` catches an infrastructure failure, logs it with `workflowId`, and returns. It does not mark the step `FAILED`. `execute` commits the choice, then `KafkaTaskPublisher` sends the task. The publish waits for the broker ack, up to 10 seconds, and does not wait for the worker.

```mermaid
flowchart TD
  run["WorkflowExecutor.execute<br/>runtime/WorkflowExecutor.java<br/>One transaction, then publish outside it"]
  choose["WorkflowExecutor.choose<br/>runtime/WorkflowExecutor.java<br/>WorkflowInstanceRepository.findById<br/>Stop when status is COMPLETED, FAILED, or COMPENSATED<br/>WorkflowDefinitionRegistry.findByType"]
  walk["WorkflowExecutor.choose<br/>runtime/WorkflowExecutor.java<br/>Walk WorkflowStepEntity rows by position<br/>Skip COMPLETED, COMPENSATED, and FAILED<br/>While COMPENSATING, skip steps that are not compensators"]
  start["WorkflowExecutor.startStep<br/>runtime/WorkflowExecutor.java<br/>Step PENDING to RUNNING, attempt plus 1<br/>deadline_at = Clock plus OrderWorkflowDefinition.STEP_TIMEOUT<br/>next_attempt_at = Clock plus republish-after-ms<br/>Instance RUNNING, unless it is already COMPENSATING<br/>current_step = this step<br/>started_at = PostgreSQL now()"]
  due{"WorkflowExecutor.republishIfDue<br/>runtime/WorkflowExecutor.java<br/>Step is RUNNING"}
  timeout["WorkflowExecutor.failStep<br/>runtime/WorkflowExecutor.java<br/>Called from republishIfDue<br/>error TIMED_OUT, attempt unchanged<br/>Returns null, so nothing is published<br/>COMPENSATING waits for RecoveryScanner"]
  wait["WorkflowExecutor.republishIfDue<br/>runtime/WorkflowExecutor.java<br/>next_attempt_at is still in the future<br/>Returns null, nothing is published"]
  again["WorkflowExecutor.republishIfDue<br/>runtime/WorkflowExecutor.java<br/>Same attempt, no new write"]
  task["WorkflowExecutor.contextFor<br/>runtime/WorkflowExecutor.java<br/>Builds api.activity.ActivityContext<br/>engine-api/src/main/java/com/workflowengine/api/activity/ActivityContext.java<br/>workflow id, type, definition version, step, attempt<br/>workflowInputJson from input_json<br/>stepInputJson null"]
  kafka["KafkaTaskPublisher.publish<br/>runtime/KafkaTaskPublisher.java<br/>Topic activity.tasks<br/>Key is the workflow id<br/>JacksonJsonSerializer writes ActivityContext<br/>acks all, wait up to 10 seconds"]

  run --> choose --> walk
  walk -->|"next step is PENDING"| start --> task --> kafka
  walk -->|"next step is RUNNING"| due
  due -->|"deadline_at due"| timeout
  due -->|"backoff not elapsed"| wait
  due -->|"backoff elapsed"| again --> task
```

The first step-start is the transaction that moves the instance from `PENDING` to `RUNNING`. Later step-starts leave the instance `RUNNING` and move `current_step`. Every committed change of the instance row bumps JPA `@Version`. Hibernate is the only writer of `version`.

## Apply the worker result

The worker is a separate process. It is on this path because the engine's next class runs only after that result arrives. `ActivityResultListener` uses consumer group `workflow-engine-results`.

```mermaid
flowchart TD
  task["Topic activity.tasks<br/>Value is ActivityContext"]
  listen["TaskListener.onTask<br/>worker/src/main/java/com/workflowengine/worker/TaskListener.java<br/>Group workflow-worker-tasks"]
  store["ActivityIdempotencyStore.find<br/>worker/src/main/java/com/workflowengine/worker/idempotency/ActivityIdempotencyStore.java<br/>Key workflowId, stepName, attempt<br/>Table activity_completion"]
  hit["TaskListener.onTask<br/>worker/src/main/java/com/workflowengine/worker/TaskListener.java<br/>Publish the stored ActivityCompletion<br/>Activity.execute is not called"]
  invoke["WorkerActivityInvoker.invoke<br/>worker/src/main/java/com/workflowengine/worker/WorkerActivityInvoker.java<br/>Activity beans in com.workflowengine.worker.activity<br/>Lookup miss: UNKNOWN_ACTIVITY<br/>Thrown exception: ACTIVITY_EXCEPTION<br/>Stub success or STUB_FORCED_FAILURE"]
  record["ActivityIdempotencyStore.record<br/>worker/src/main/java/com/workflowengine/worker/idempotency/ActivityIdempotencyStore.java<br/>Commit the completion row<br/>TaskListener then publishes it"]
  results["Topic activity.results<br/>Value is ActivityCompletion"]
  result["ActivityResultListener.onResult<br/>runtime/ActivityResultListener.java<br/>Group workflow-engine-results<br/>Calls WorkflowExecutor.onActivityResult"]
  apply["WorkflowExecutor.applyResult<br/>runtime/WorkflowExecutor.java<br/>Step must still be RUNNING<br/>Instance RUNNING or COMPENSATING<br/>Attempt must match<br/>Otherwise skip the write"]
  fail["WorkflowExecutor.failStep<br/>runtime/WorkflowExecutor.java<br/>Step FAILED, completed_at = now()<br/>error copied onto the instance<br/>output_json stays null"]
  plan{"WorkflowExecutor.failStep<br/>runtime/WorkflowExecutor.java<br/>Calls planCompensation when the instance was RUNNING"}
  comp["WorkflowExecutor.planCompensation<br/>runtime/WorkflowExecutor.java<br/>Insert COMPENSATE_* rows<br/>Reverse position order, status PENDING, attempt 0<br/>Instance COMPENSATING<br/>onActivityResult calls WorkflowExecutor.run"]
  terminalFail["WorkflowExecutor.failStep<br/>runtime/WorkflowExecutor.java<br/>Instance FAILED<br/>Walk stops<br/>Later steps stay PENDING"]
  compFail["WorkflowExecutor.failStep<br/>runtime/WorkflowExecutor.java<br/>Instance was already COMPENSATING<br/>Instance FAILED<br/>Remaining compensation rows stay PENDING"]
  mid["WorkflowExecutor.completeMidStep<br/>runtime/WorkflowExecutor.java<br/>Step COMPLETED, output stored<br/>OPTIMISTIC_FORCE_INCREMENT bumps version<br/>markForwardCompensated when this step is a compensator<br/>onActivityResult calls WorkflowExecutor.run"]
  lastFwd["WorkflowExecutor.completeLastStep<br/>runtime/WorkflowExecutor.java<br/>Last forward step and instance COMPLETED<br/>in one transaction<br/>Instance output_json copies the last output<br/>Walk stops. Happy path version is 10"]
  lastComp["WorkflowExecutor.completeCompensation<br/>runtime/WorkflowExecutor.java<br/>Compensation step COMPLETED<br/>markForwardCompensated sets the forward step COMPENSATED<br/>Instance COMPENSATED<br/>output_json stays null<br/>error stays the original forward failure<br/>Walk stops"]

  task --> listen --> store
  store -->|"row exists"| hit --> results
  store -->|miss| invoke --> record --> results
  results --> result --> apply
  apply -->|"success is false"| fail --> plan
  plan -->|yes| comp
  plan -->|"no completed step to undo"| terminalFail
  plan -->|"instance already COMPENSATING"| compFail
  apply -->|"success, not the last step"| mid
  apply -->|"success, last forward step"| lastFwd
  apply -->|"success, last compensation step"| lastComp
```

`failStep` returns without writing when the step is no longer `RUNNING`. A timeout that committed first keeps `TIMED_OUT`. The late success or failure is discarded, and the walk does not continue.

A forward failure of `CREATE_ORDER` has no completed step to undo, so the instance stays `FAILED` at `version` 2. A failure of `PROCESS_PAYMENT` after two completed steps inserts `COMPENSATE_RESERVE_INVENTORY` and `COMPENSATE_CREATE_ORDER`, ends `COMPENSATED`, and reaches `version` 10. While `COMPENSATING`, `choose` does not start `CREATE_SHIPMENT` or `SEND_NOTIFICATION`.

## Resume a leftover, and time out a step

These classes do not run on the POST thread. They call the same executor after the request has returned, or after a restart.

```mermaid
flowchart TD
  readyScan["RecoveryScanner.onReady<br/>RecoveryScanner.scheduled<br/>runtime/RecoveryScanner.java<br/>ApplicationReadyEvent, then every recovery interval<br/>Thread schedule- from WorkflowExecutionConfig"]
  scan["RecoveryScanner.scan<br/>runtime/RecoveryScanner.java<br/>WorkflowInstanceRepository.findByStatusIn<br/>Statuses PENDING, RUNNING, COMPENSATING"]
  pending["RecoveryScanner.due<br/>runtime/RecoveryScanner.java<br/>PENDING and every step PENDING<br/>WorkflowDispatcher.submit"]
  gap["RecoveryScanner.due<br/>runtime/RecoveryScanner.java<br/>RUNNING or COMPENSATING<br/>no RUNNING step, a PENDING step remains<br/>WorkflowDispatcher.submit"]
  republish["RecoveryScanner.due<br/>runtime/RecoveryScanner.java<br/>RUNNING step inside deadline_at and next_attempt_at due<br/>WorkflowDispatcher.submit<br/>WorkflowExecutor.republishIfDue keeps the attempt"]
  skip["RecoveryScanner.due<br/>runtime/RecoveryScanner.java<br/>deadline_at already due<br/>Does not submit<br/>TimeoutPoller writes TIMED_OUT"]
  readyPoll["TimeoutPoller.onReady<br/>TimeoutPoller.scheduled<br/>runtime/TimeoutPoller.java<br/>ApplicationReadyEvent, then every timeout interval<br/>Thread schedule- from WorkflowExecutionConfig"]
  poll["TimeoutPoller.scan<br/>runtime/TimeoutPoller.java<br/>Statuses RUNNING and COMPENSATING<br/>Does not use WorkflowDispatcher"]
  timeout["WorkflowExecutor.timeoutIfDue<br/>runtime/WorkflowExecutor.java<br/>applyTimeoutIfDue calls failStep<br/>error TIMED_OUT, attempt unchanged<br/>If that write sets COMPENSATING, call run<br/>so the first compensation step starts"]

  readyScan --> scan
  scan --> pending
  scan --> gap
  scan --> republish
  scan --> skip
  readyPoll --> poll --> timeout
```

`WorkflowDispatcher` keeps an in-memory inflight set so the scanner does not start a second `run` while this process is already inside one. After a crash the set is empty, and the next scan submits the leftover. `FAILED`, `COMPLETED`, and `COMPENSATED` are terminal. The scanner does not select them, and an idempotent `POST` does not submit them either.

`GET` is separate from this loop.

```mermaid
flowchart TD
  get["WorkflowController.get<br/>web/WorkflowController.java<br/>GET /api/v1/workflows/id"]
  load["GetWorkflowService.get<br/>application/GetWorkflowService.java<br/>WorkflowInstanceRepository.findById<br/>Steps ordered by position<br/>Returns the committed snapshot, including a leftover"]
  body["WorkflowResponses.from<br/>web/WorkflowResponses.java<br/>HTTP 200 WorkflowResponse"]
  missing["RestExceptionHandler.notFound<br/>web/RestExceptionHandler.java<br/>WorkflowNotFoundException becomes 404"]

  get --> load
  load -->|row exists| body
  load -->|no row| missing
```

## Classes

Engine paths are under `engine/src/main/java/com/workflowengine/`. Worker and `engine-api` rows use their full path. Each diagram block starts with `Class.method` and names that file.

| Class | Source | Thread | What it does on this path |
|---|---|---|---|
| `BodySizeFilter` | `web/BodySizeFilter.java` | HTTP | Rejects an `/api/` body over 64 KB with 413. |
| `WorkflowController` | `web/WorkflowController.java` | HTTP | Admits on POST and loads a snapshot on GET. |
| `WorkflowResponses` | `web/WorkflowResponses.java` | HTTP | Turns a snapshot into the JSON body. |
| `RestExceptionHandler` | `web/RestExceptionHandler.java` | HTTP | Generic 400, 404, 413, 503, and 500 bodies. |
| `StartWorkflowService` | `application/StartWorkflowService.java` | HTTP | Validates, commits the admit transaction, submits only the insert winner. |
| `AdmissionResult` | `application/AdmissionResult.java` | HTTP | `created` true is the 201 path. `created` false is the 200 path. |
| `GetWorkflowService` | `application/GetWorkflowService.java` | HTTP | Returns the committed snapshot for GET. |
| `WorkflowDefinitionRegistry` | `domain/WorkflowDefinitionRegistry.java` | any | Maps `ORDER` to `OrderWorkflowDefinition`. |
| `OrderWorkflowDefinition` | `definition/OrderWorkflowDefinition.java` | any | Five forward steps, each with a compensator and a 5-minute timeout. |
| `WorkflowInstanceRepository` | `persistence/WorkflowInstanceRepository.java` | any | Loads and inserts the instance and its steps. |
| `WorkflowExecutionConfig` | `config/WorkflowExecutionConfig.java` | startup | Builds the `workflow-` pool and the `schedule-` thread. |
| `WorkflowDispatcher` | `runtime/WorkflowDispatcher.java` | HTTP or `schedule-` | One inflight `run` per id on `workflowTaskExecutor`. |
| `WorkflowExecutor` | `runtime/WorkflowExecutor.java` | `workflow-*`, result listener, or `schedule-` | `choose`, `startStep`, `republishIfDue`, `applyResult`, `failStep`, `planCompensation`, `completeMidStep`, `completeLastStep`, `completeCompensation`, `timeoutIfDue`. |
| `KafkaTaskPublisher` | `runtime/KafkaTaskPublisher.java` | `workflow-*` | Sends `ActivityContext` after the step-start commit. |
| `ActivityResultListener` | `runtime/ActivityResultListener.java` | Kafka listener | Delivers `ActivityCompletion` to `onActivityResult`. |
| `RecoveryScanner` | `runtime/RecoveryScanner.java` | `schedule-` | `scan` and `due` submit a never-started admit or a due running step. |
| `TimeoutPoller` | `runtime/TimeoutPoller.java` | `schedule-` | `scan` asks `timeoutIfDue` to fail a due `RUNNING` step with `TIMED_OUT`. |
| `TaskListener` | `worker/src/main/java/com/workflowengine/worker/TaskListener.java` | worker listener | Consumes a task, skips a stored attempt, publishes `ActivityCompletion`. |
| `ActivityIdempotencyStore` | `worker/src/main/java/com/workflowengine/worker/idempotency/ActivityIdempotencyStore.java` | worker listener | `find` and `record` on `activity_completion`. |
| `WorkerActivityInvoker` | `worker/src/main/java/com/workflowengine/worker/WorkerActivityInvoker.java` | worker listener | Looks up the `Activity` bean and calls `execute`. |
| `ActivityContext` | `engine-api/src/main/java/com/workflowengine/api/activity/ActivityContext.java` | Kafka | Task record. JSON inside it stays text. |
| `ActivityCompletion` | `engine-api/src/main/java/com/workflowengine/api/activity/ActivityCompletion.java` | Kafka | Result record. |
