# Engine code flow

The `engine` module is the Spring Boot process that admits an ORDER workflow and moves it forward one committed Postgres transaction at a time. This page follows a start request through the classes that handle it.

Keep this file in the same change as the code it draws. When a class in the table below changes its thread, transaction, status write, publish, or the next class it calls, update the matching diagram and the table row. That includes the worker classes drawn on the result path (`TaskListener`, `ActivityIdempotencyStore`, `WorkerActivityInvoker`). `AGENTS.md` requires the update. A diagram that no longer matches the code is a bug.

A client posts to `POST /api/v1/workflows`. The request thread commits the instance as `PENDING` and returns. A `workflow-*` thread then starts one step and publishes `ActivityContext` on `activity.tasks`. The worker runs the activity. The engine applies `ActivityCompletion` from `activity.results` and either starts the next step, compensates, or stops.

Postgres is the source of truth. Each transition commits before the next publish. The happy path ends at instance `COMPLETED` with `version` 10. `GET /api/v1/workflows/{id}` is how a client sees that progress. The `201` body is the admit snapshot and stays `PENDING` / `version` 0.

## Admit the request

`BodySizeFilter` runs first on the Tomcat thread. `WorkflowController` turns the JSON body into a `StartWorkflowCommand`. `StartWorkflowService` validates it, inserts the rows, and only the insert winner calls `WorkflowDispatcher`.

```mermaid
flowchart TD
  post["Client POST /api/v1/workflows"]
  filter["BodySizeFilter<br/>POST, PUT, and PATCH under /api/<br/>Content-Length or counted bytes over 65536 returns 413"]
  ctrl["WorkflowController.start<br/>Requires an object input<br/>Jackson 3 writes input to a JSON string<br/>Builds StartWorkflowCommand"]
  validate["StartWorkflowService.validate<br/>WorkflowDefinitionRegistry.findByType<br/>ORDER only, via OrderWorkflowDefinition<br/>idempotencyKey length 1..128<br/>inputJson required"]
  bad["RestExceptionHandler<br/>400 Bad Request<br/>503 when Postgres is unreachable<br/>Generic body, no SQL"]
  admit["StartWorkflowService.insert<br/>Admit transaction<br/>New UUID, definition_version 1<br/>Instance PENDING, version 0, current_step null<br/>Five steps PENDING, attempt 0, position 0..4<br/>WorkflowInstanceRepository.saveAndFlush"]
  race{"Unique index<br/>uq_workflow_instance_idempotency_key"}
  existing["Reload by idempotency key<br/>AdmissionResult created false<br/>WorkflowResponses.from<br/>HTTP 200 current snapshot<br/>Dispatcher is not called"]
  submit["WorkflowDispatcher.submit<br/>Inflight set rejects a second submit<br/>workflowTaskExecutor runs the rest<br/>Threads are named workflow-"]
  created["HTTP 201<br/>Location /api/v1/workflows/id<br/>Body is the in-memory admit snapshot<br/>Still PENDING, version 0"]
  run["WorkflowExecutor.run"]

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
  run["WorkflowExecutor.execute<br/>One transaction, then publish outside it"]
  choose["choose<br/>Load the instance with steps<br/>Stop when status is COMPLETED, FAILED, or COMPENSATED<br/>Resolve the definition"]
  walk["Walk steps by position<br/>Skip COMPLETED, COMPENSATED, and FAILED<br/>While COMPENSATING, also skip forward steps"]
  start["startStep<br/>Step PENDING to RUNNING, attempt plus 1<br/>deadline_at = clock plus STEP_TIMEOUT<br/>next_attempt_at = clock plus republish-after-ms<br/>Instance RUNNING, unless it is already COMPENSATING<br/>current_step = this step<br/>started_at = PostgreSQL now()"]
  due{"RUNNING step"}
  timeout["failStep with error TIMED_OUT<br/>attempt stays the same<br/>Nothing is published<br/>COMPENSATING waits for RecoveryScanner"]
  wait["next_attempt_at is still in the future<br/>Nothing is published"]
  again["republishIfDue<br/>Same attempt, no new write"]
  task["ActivityContext<br/>workflow id, type, definition version<br/>step name, attempt<br/>workflowInputJson from input_json<br/>stepInputJson null"]
  kafka["KafkaTaskPublisher.publish<br/>Topic activity.tasks<br/>Key is the workflow id<br/>JacksonJsonSerializer writes the record<br/>acks all, wait up to 10 seconds"]

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
  task["activity.tasks"]
  listen["TaskListener.onTask<br/>Group workflow-worker-tasks"]
  store["ActivityIdempotencyStore.find<br/>Key workflowId, stepName, attempt"]
  hit["Publish the stored ActivityCompletion<br/>Activity.execute is not called"]
  invoke["WorkerActivityInvoker.invoke<br/>Lookup miss: UNKNOWN_ACTIVITY<br/>Thrown exception: ACTIVITY_EXCEPTION<br/>Stub success or STUB_FORCED_FAILURE"]
  record["ActivityIdempotencyStore.record<br/>Commit the completion row, then publish"]
  results["activity.results"]
  result["ActivityResultListener.onResult<br/>WorkflowExecutor.onActivityResult"]
  apply["applyResult<br/>Step must still be RUNNING<br/>Instance RUNNING or COMPENSATING<br/>Attempt must match<br/>Otherwise skip the write"]
  fail["failStep<br/>Step FAILED, completed_at = now()<br/>error copied onto the instance<br/>output_json stays null"]
  plan{"Instance was RUNNING<br/>and an earlier step is COMPLETED<br/>with a compensator?"}
  comp["planCompensation<br/>Insert COMPENSATE_* rows<br/>Reverse position order, status PENDING, attempt 0<br/>Instance COMPENSATING<br/>Then run again"]
  terminalFail["Instance FAILED<br/>Walk stops<br/>Later steps stay PENDING"]
  compFail["Already COMPENSATING<br/>Instance FAILED<br/>Remaining compensation rows stay PENDING"]
  mid["completeMidStep<br/>Step COMPLETED, output stored<br/>OPTIMISTIC_FORCE_INCREMENT bumps version<br/>If this step is a compensator,<br/>markForwardCompensated sets the forward step COMPENSATED<br/>Then run again"]
  lastFwd["completeLastStep<br/>Last forward step and instance COMPLETED<br/>in one transaction<br/>Instance output_json copies the last output<br/>Walk stops. Happy path version is 10"]
  lastComp["completeCompensation<br/>Compensation step COMPLETED<br/>Matching forward step COMPENSATED<br/>Instance COMPENSATED<br/>output_json stays null<br/>error stays the original forward failure<br/>Walk stops"]

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
  ready["ApplicationReadyEvent<br/>and every 2 seconds<br/>Scheduler thread schedule-"]
  scan["RecoveryScanner.scan<br/>Statuses PENDING, RUNNING, COMPENSATING"]
  pending["PENDING and every step PENDING<br/>WorkflowDispatcher.submit"]
  gap["RUNNING or COMPENSATING<br/>no RUNNING step, a PENDING step remains<br/>WorkflowDispatcher.submit"]
  republish["RUNNING step inside deadline_at<br/>and next_attempt_at due<br/>WorkflowDispatcher.submit<br/>Executor republishes the same attempt"]
  skip["deadline_at already due<br/>Scanner does not submit"]
  poll["TimeoutPoller.scan<br/>Statuses RUNNING and COMPENSATING<br/>Does not use WorkflowDispatcher"]
  timeout["WorkflowExecutor.timeoutIfDue<br/>failStep error TIMED_OUT<br/>attempt unchanged<br/>If that write sets COMPENSATING, call run<br/>so the first compensation step starts"]

  ready --> scan
  scan --> pending
  scan --> gap
  scan --> republish
  scan --> skip
  ready --> poll --> timeout
```

`WorkflowDispatcher` keeps an in-memory inflight set so the scanner does not start a second `run` while this process is already inside one. After a crash the set is empty, and the next scan submits the leftover. `FAILED`, `COMPLETED`, and `COMPENSATED` are terminal. The scanner does not select them, and an idempotent `POST` does not submit them either.

`GET` is separate from this loop. `GetWorkflowService.get` loads the instance and its steps in position order and returns that snapshot, including a leftover. `WorkflowController.get` maps a missing id to `404`.

## Classes

| Class | Thread | What it does on this path |
|---|---|---|
| `BodySizeFilter` | HTTP | Rejects an `/api/` body over 64 KB with 413. |
| `WorkflowController` | HTTP | Admits on POST and loads a snapshot on GET. |
| `StartWorkflowService` | HTTP | Validates, commits the admit transaction, submits only the insert winner. |
| `WorkflowDefinitionRegistry` | any | Maps `ORDER` to `OrderWorkflowDefinition`. |
| `RestExceptionHandler` | HTTP | Generic 400, 404, 413, 503, and 500 bodies. |
| `WorkflowDispatcher` | HTTP or `schedule-` | One inflight `run` per id on `workflowTaskExecutor`. |
| `WorkflowExecutor` | `workflow-*`, result listener, or `schedule-` | Commits step-start, republish, complete, fail, and compensation. |
| `KafkaTaskPublisher` | `workflow-*` | Sends `ActivityContext` after the step-start commit. |
| `ActivityResultListener` | Kafka listener | Delivers `ActivityCompletion` to `onActivityResult`. |
| `RecoveryScanner` | `schedule-` | Submits a never-started admit or a due running step. |
| `TimeoutPoller` | `schedule-` | Fails a due `RUNNING` step with `TIMED_OUT`. |
| `GetWorkflowService` | HTTP | Returns the committed snapshot for GET. |
