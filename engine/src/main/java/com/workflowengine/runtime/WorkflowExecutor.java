package com.workflowengine.runtime;

import com.workflowengine.api.StepStatus;
import com.workflowengine.api.WorkflowStatus;
import com.workflowengine.api.activity.ActivityCompletion;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;
import com.workflowengine.domain.StepDefinition;
import com.workflowengine.domain.WorkflowDefinition;
import com.workflowengine.domain.WorkflowDefinitionRegistry;
import com.workflowengine.history.HistoryEventType;
import com.workflowengine.history.WorkflowHistory;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import com.workflowengine.persistence.WorkflowStepEntity;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Dispatches a linear ORDER saga: commit {@code RUNNING}, publish a task, return.
 *
 * <p>Each state change is its own committed transaction. Nothing in this class
 * calls {@code Activity.execute}. The worker does that and
 * {@link #onActivityResult} applies the completion later. Instance
 * {@code PENDING→RUNNING} is folded into the first step-start transaction.
 * Last-step {@code COMPLETED} and instance {@code COMPLETED} share one
 * workflow-complete transaction.
 *
 * <p><strong>Leftovers:</strong> a never-started {@code PENDING} admission
 * starts the first step and publishes. A {@code RUNNING} step inside its
 * deadline is republished with the same {@code attempt} once
 * {@code next_attempt_at} is due. A due {@code deadline_at} becomes
 * {@code FAILED} with {@link #TIMED_OUT_ERROR}. {@code FAILED} stays terminal.
 * Republish does not increment {@code attempt}. The deadline bounds a lost
 * result. A crash between a mid-step complete and the next step-start leaves
 * instance {@code RUNNING} with a {@code PENDING} step and no {@code RUNNING}
 * step; the next {@link #run} starts that step.
 *
 * <p>Runs on {@code workflow-*} threads and on the result-listener thread.
 * {@link #timeoutIfDue} also runs on the scheduler thread. This class must not
 * import the worker stub package. Infrastructure failures are logged and
 * stop the run; they do not mark the step {@code FAILED}. A completion that
 * arrives after a timeout is discarded.
 *
 * @implNote Happy-path terminal {@code version == 10}. Mid-step success TXs
 * bump {@code @Version} with {@link LockModeType#OPTIMISTIC_FORCE_INCREMENT}.
 */
@Slf4j
@Component
public class WorkflowExecutor {

    /**
     * Step and instance {@code error} when {@code deadline_at} is due.
     * Status stays {@code FAILED}. {@code attempt} is not incremented.
     */
    public static final String TIMED_OUT_ERROR = "TIMED_OUT";

    private static final int TIMEOUT_STRIPES = 32;

    private final WorkflowInstanceRepository instances;
    private final WorkflowDefinitionRegistry definitions;
    private final TaskPublisher publisher;
    private final TransactionTemplate tx;
    private final EntityManager entityManager;
    private final WorkflowHistory history;
    private final Clock clock;
    private final Duration republishAfter;

    /**
     * Serializes {@link #timeoutIfDue} so the ready-event pass and the
     * scheduler pass cannot both flush a timeout for the same instance.
     */
    private final Object[] timeoutStripes;

    /**
     * Builds an executor that opens its own {@link TransactionTemplate} per unit of work.
     *
     * @param instances instance repository
     * @param definitions type registry
     * @param publisher task sender used only after the step-start transaction commits
     * @param transactionManager not used as a class-level {@code @Transactional}
     * @param entityManager used to force {@code @Version} bumps and stamp timestamps
     * @param history appends one event per committed transition in the same transaction
     * @param clock republish due-time and step-deadline clock
     * @param republishAfterMs delay stored in {@code next_attempt_at} at step-start;
     * zero means the scanner may republish immediately
     */
    public WorkflowExecutor(
            WorkflowInstanceRepository instances,
            WorkflowDefinitionRegistry definitions,
            TaskPublisher publisher,
            PlatformTransactionManager transactionManager,
            EntityManager entityManager,
            WorkflowHistory history,
            Clock clock,
            @Value("${workflow.dispatch.republish-after-ms:30000}") long republishAfterMs
    ) {
        this.instances = instances;
        this.definitions = definitions;
        this.publisher = publisher;
        this.tx = new TransactionTemplate(transactionManager);
        this.entityManager = entityManager;
        this.history = history;
        this.clock = clock;
        this.republishAfter = Duration.ofMillis(Math.max(republishAfterMs, 0));
        this.timeoutStripes = new Object[TIMEOUT_STRIPES];
        for (int i = 0; i < TIMEOUT_STRIPES; i++) {
            this.timeoutStripes[i] = new Object();
        }
    }

    /**
     * Executes the saga for {@code workflowId} on a {@code workflow-*} thread.
     *
     * @param workflowId instance created by admission
     * @implNote Catches {@link RuntimeException} from persistence or unexpected
     * transitions, logs ERROR, and returns. Does not convert that into step
     * {@code FAILED}.
     */
    public void run(UUID workflowId) {
        try {
            execute(workflowId);
        } catch (RuntimeException ex) {
            log.error("executor infrastructure failure workflowId={}", workflowId, ex);
        }
    }

    /**
     * Applies one worker result, then starts the next step when the walk continues.
     *
     * <p>Called on the Kafka listener thread with no open workflow transaction.
     * An optimistic-lock loss is logged and ignored. Any other persistence
     * failure propagates so the broker can redeliver the result.
     *
     * @param completion decoded result; not null
     */
    public void onActivityResult(ActivityCompletion completion) {
        Boolean continueWalk;
        try {
            continueWalk = tx.execute(status -> applyResult(completion));
        } catch (OptimisticLockingFailureException ex) {
            log.error("executor infrastructure failure workflowId={} reason=result",
                    completion.workflowId(), ex);
            return;
        }
        if (Boolean.TRUE.equals(continueWalk)) {
            run(completion.workflowId());
        }
    }

    /**
     * Starts the next pending step or republishes the current {@code RUNNING}
     * attempt, then publishes outside the transaction.
     *
     * @param workflowId instance id
     */
    private void execute(UUID workflowId) {
        ActivityContext task = tx.execute(status -> choose(workflowId));
        if (task == null) {
            return;
        }
        publisher.publish(task);
        log.info("task published workflowId={} type={} step={} attempt={} status=RUNNING",
                task.workflowId(), task.workflowType(), task.stepName(), task.attempt());
    }

    /**
     * Step-fail transaction for a due {@code deadline_at}, if the step is still
     * {@code RUNNING}. Does not invoke the activity and does not increment
     * {@code attempt}.
     *
     * <p>Called on the scheduler thread with no open workflow transaction.
     * Startup runs {@code onReady} and the first scheduled tick together, so
     * callers for the same id are serialized. The second pass sees
     * {@code FAILED} and does not write. A concurrent completion from the
     * executor thread still loses or wins on {@code @Version}. That loser
     * logs an infrastructure failure and stops; the committed row stands.
     *
     * @param workflowId instance id
     */
    public void timeoutIfDue(UUID workflowId) {
        synchronized (timeoutStripe(workflowId)) {
            try {
                tx.executeWithoutResult(status -> applyTimeoutIfDue(workflowId));
            } catch (RuntimeException ex) {
                log.error("executor infrastructure failure workflowId={} reason=timeout", workflowId, ex);
                return;
            }
        }
        if (compensating(workflowId)) {
            run(workflowId);
        }
    }

    /**
     * Lock stripe for {@link #timeoutIfDue}. Not a per-id lock; two workflows
     * can share a stripe and wait briefly.
     *
     * @param workflowId instance id
     * @return monitor for that stripe
     */
    private Object timeoutStripe(UUID workflowId) {
        int index = (workflowId.hashCode() & Integer.MAX_VALUE) % TIMEOUT_STRIPES;
        return timeoutStripes[index];
    }

    /**
     * True when {@code deadline} is non-null and not after {@code now}.
     * Equal instants are due. Null means the step has no timeout.
     *
     * @param deadline {@code workflow_step.deadline_at}; may be null
     * @param now engine clock instant
     * @return whether the poller or a running-resume should fail the attempt
     */
    static boolean deadlineDue(Instant deadline, Instant now) {
        return deadline != null && !deadline.isAfter(now);
    }

    /**
     * Picks the one task to publish: start the next {@code PENDING} step, or
     * republish a due {@code RUNNING} step without incrementing {@code attempt}.
     *
     * @param workflowId instance id
     * @return task to publish, or null when the walk should stop
     */
    private ActivityContext choose(UUID workflowId) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElse(null);
        if (instance == null) {
            log.error("executor infrastructure failure workflowId={} reason=not_found", workflowId);
            return null;
        }
        if (instance.getStatus() == WorkflowStatus.COMPLETED
                || instance.getStatus() == WorkflowStatus.FAILED
                || instance.getStatus() == WorkflowStatus.COMPENSATED) {
            return null;
        }
        WorkflowDefinition definition = definitions.findByType(instance.getType()).orElse(null);
        if (definition == null) {
            log.error("executor infrastructure failure workflowId={} reason=unknown_type type={}",
                    workflowId, instance.getType());
            return null;
        }
        boolean compensating = instance.getStatus() == WorkflowStatus.COMPENSATING;
        List<WorkflowStepEntity> ordered = instance.getSteps().stream()
                .sorted(java.util.Comparator.comparingInt(WorkflowStepEntity::getPosition))
                .toList();
        for (WorkflowStepEntity step : ordered) {
            if (step.getStatus() == StepStatus.COMPLETED || step.getStatus() == StepStatus.COMPENSATED) {
                continue;
            }
            if (step.getStatus() == StepStatus.FAILED) {
                continue;
            }
            if (compensating && !isCompensation(definition, step.getName())) {
                continue;
            }
            if (step.getStatus() == StepStatus.PENDING) {
                int attempt = startStep(instance, step, step.getName(), timeoutFor(definition, step.getName()));
                return contextFor(instance, step.getName(), attempt);
            }
            return republishIfDue(instance, step);
        }
        return null;
    }

    /**
     * Applies a result when the named step is still {@code RUNNING} at that attempt.
     *
     * @param completion worker outcome
     * @return true when the mid-step complete committed and the next step should start
     */
    private boolean applyResult(ActivityCompletion completion) {
        WorkflowInstanceEntity instance = instances.findById(completion.workflowId()).orElse(null);
        if (instance == null) {
            log.error("executor infrastructure failure workflowId={} reason=not_found", completion.workflowId());
            return false;
        }
        WorkflowDefinition definition = definitions.findByType(instance.getType()).orElse(null);
        if (definition == null) {
            log.error("executor infrastructure failure workflowId={} reason=unknown_type type={}",
                    completion.workflowId(), instance.getType());
            return false;
        }
        WorkflowStepEntity step = instance.getSteps().stream()
                .filter(candidate -> candidate.getName().equals(completion.stepName()))
                .findFirst()
                .orElse(null);
        if (step == null) {
            log.error("executor infrastructure failure workflowId={} reason=unknown_step step={}",
                    completion.workflowId(), completion.stepName());
            return false;
        }
        if (step.getStatus() != StepStatus.RUNNING
                || !forwardOrCompensating(instance.getStatus())
                || step.getAttempt() != completion.attempt()) {
            log.info("step write skipped workflowId={} type={} step={} attempt={} status={} version={} reason=not_running",
                    instance.getId(), instance.getType(), step.getName(), step.getAttempt(),
                    step.getStatus(), instance.getVersion());
            return false;
        }
        ActivityResult result = new ActivityResult(
                completion.success(), completion.outputJson(), completion.error());
        int lastPosition = instance.getSteps().stream()
                .mapToInt(WorkflowStepEntity::getPosition)
                .max()
                .orElseThrow();
        boolean last = step.getPosition() == lastPosition;
        if (!completion.success()) {
            boolean wrote = failStep(completion.workflowId(), step.getPosition(), result);
            return wrote && compensating(completion.workflowId());
        }
        if (last && isCompensation(definition, step.getName())) {
            completeCompensation(completion.workflowId(), step.getPosition(), result);
            return false;
        }
        if (last) {
            completeLastStep(completion.workflowId(), step.getPosition(), result);
            return false;
        }
        return completeMidStep(completion.workflowId(), step.getPosition(), result);
    }

    /**
     * Step-start transaction: step {@code PENDING→RUNNING}, increment {@code attempt}, set
     * instance {@code RUNNING} and {@code current_step}, stamp {@code started_at}
     * from the database, {@code deadline_at} from the engine clock, and
     * {@code next_attempt_at} as the earliest republish time.
     *
     * @param instance loaded aggregate
     * @param step {@code PENDING} step
     * @param stepName definition name
     * @param timeout per-attempt limit; null means no deadline
     * @return attempt after increment
     */
    private int startStep(
            WorkflowInstanceEntity instance,
            WorkflowStepEntity step,
            String stepName,
            Duration timeout
    ) {
        int attempt = step.getAttempt() + 1;
        Instant now = clock.instant();
        step.setStatus(StepStatus.RUNNING);
        step.setAttempt(attempt);
        step.setNextAttemptAt(republishAfter.isZero() ? null : now.plus(republishAfter));
        step.setDeadlineAt(deadlineFor(now, timeout));
        if (instance.getStatus() != WorkflowStatus.COMPENSATING) {
            instance.setStatus(WorkflowStatus.RUNNING);
        }
        instance.setCurrentStep(stepName);
        stampStartedAt(step);
        history.append(instance, HistoryEventType.STEP_STARTED, step, null);

        log.info("step started workflowId={} type={} step={} attempt={} status={} version={}",
                instance.getId(), instance.getType(), stepName, attempt, instance.getStatus(), instance.getVersion());
        return attempt;
    }

    /**
     * Republishes a leftover {@code RUNNING} step, or fails it when the deadline is due.
     * The attempt is not incremented. A future {@code next_attempt_at} waits.
     *
     * @param instance loaded aggregate
     * @param step the {@code RUNNING} step
     * @return task for the current attempt, or null when nothing is published
     */
    private ActivityContext republishIfDue(WorkflowInstanceEntity instance, WorkflowStepEntity step) {
        Instant now = clock.instant();
        if (deadlineDue(step.getDeadlineAt(), now)) {
            failStep(instance.getId(), step.getPosition(),
                    new ActivityResult(false, null, TIMED_OUT_ERROR));
            return null;
        }
        Instant next = step.getNextAttemptAt();
        if (next != null && next.isAfter(now)) {
            return null;
        }
        log.info("task republish workflowId={} type={} step={} attempt={} status=RUNNING version={}",
                instance.getId(), instance.getType(), step.getName(), step.getAttempt(), instance.getVersion());
        return contextFor(instance, step.getName(), step.getAttempt());
    }

    /**
     * Builds the task body from the instance row so every step sees the same input.
     *
     * @param instance loaded aggregate
     * @param stepName activity name
     * @param attempt attempt stored on the step
     * @return context with null step input
     */
    private static ActivityContext contextFor(WorkflowInstanceEntity instance, String stepName, int attempt) {
        return new ActivityContext(
                instance.getId(),
                instance.getType(),
                instance.getDefinitionVersion(),
                stepName,
                attempt,
                instance.getInputJson(),
                null
        );
    }

    /**
     * Mid-step complete transaction: step {@code COMPLETED}, force {@code @Version} bump
     * even though instance status is unchanged.
     *
     * @param workflowId instance id
     * @param position step index
     * @param result successful activity output
     * @return false when the step is no longer {@code RUNNING} (timeout won) and the success was discarded
     */
    private boolean completeMidStep(UUID workflowId, int position, ActivityResult result) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        WorkflowStepEntity step = stepAt(instance, position);
        if (!stillRunning(instance, step)) {
            return false;
        }
        step.setStatus(StepStatus.COMPLETED);
        step.setOutputJson(result.outputJson());
        step.setError(null);
        history.append(instance, HistoryEventType.STEP_COMPLETED, step, null);
        appendForwardCompensated(instance, step.getName());
        entityManager.lock(instance, LockModeType.OPTIMISTIC_FORCE_INCREMENT);
        stampCompletedAt(step);

        log.info("step completed workflowId={} type={} step={} attempt={} status=COMPLETED version={}",
                workflowId, instance.getType(), step.getName(), step.getAttempt(), instance.getVersion());
        return true;
    }

    /**
     * Workflow-complete transaction: step and instance {@code COMPLETED}, copy last
     * output onto the instance.
     *
     * @param workflowId instance id
     * @param position last step index
     * @param result successful activity output
     * @return false when the step is no longer {@code RUNNING} and the success was discarded
     */
    private boolean completeLastStep(UUID workflowId, int position, ActivityResult result) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        WorkflowStepEntity step = stepAt(instance, position);
        if (!stillRunning(instance, step)) {
            return false;
        }
        step.setStatus(StepStatus.COMPLETED);
        step.setOutputJson(result.outputJson());
        step.setError(null);
        instance.setStatus(WorkflowStatus.COMPLETED);
        instance.setCurrentStep(step.getName());
        instance.setOutputJson(result.outputJson());
        instance.setError(null);
        stampCompletedAt(step);
        history.append(instance, HistoryEventType.STEP_COMPLETED, step, null);
        history.append(instance, HistoryEventType.WORKFLOW_COMPLETED, null);

        log.info("workflow completed workflowId={} type={} step={} attempt={} status=COMPLETED version={}",
                workflowId, instance.getType(), step.getName(), step.getAttempt(), instance.getVersion());
        return true;
    }

    /**
     * Last compensation transaction: compensation step {@code COMPLETED}, the
     * forward step {@code COMPENSATED}, and the instance {@code COMPENSATED}.
     * Instance {@code output_json} stays null. The forward failure stays in
     * {@code error}.
     *
     * @param workflowId instance id
     * @param position compensation step index
     * @param result successful compensation output
     * @return false when the step is no longer {@code RUNNING}
     */
    private boolean completeCompensation(UUID workflowId, int position, ActivityResult result) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        WorkflowStepEntity step = stepAt(instance, position);
        if (!stillRunning(instance, step)) {
            return false;
        }
        step.setStatus(StepStatus.COMPLETED);
        step.setOutputJson(result.outputJson());
        step.setError(null);
        markForwardCompensated(instance, step.getName());
        instance.setStatus(WorkflowStatus.COMPENSATED);
        instance.setCurrentStep(step.getName());
        instance.setOutputJson(null);
        stampCompletedAt(step);
        history.append(instance, HistoryEventType.STEP_COMPLETED, step, null);
        appendForwardCompensated(instance, step.getName());
        history.append(instance, HistoryEventType.WORKFLOW_COMPENSATED, null);

        log.info("workflow compensated workflowId={} type={} step={} attempt={} status=COMPENSATED version={}",
                workflowId, instance.getType(), step.getName(), step.getAttempt(), instance.getVersion());
        return true;
    }

    /**
     * Step-fail transaction: step and instance {@code FAILED}. Instance {@code output_json}
     * stays null. Terminal; {@code FAILED} is not retried.
     *
     * <p>No-ops when the step or instance is no longer {@code RUNNING}. That is
     * how a late activity result loses to a timeout that already committed.
     * Must be called inside the caller's workflow transaction.
     *
     * @param workflowId instance id
     * @param position failed step index
     * @param result unsuccessful activity result, or {@code TIMED_OUT_ERROR}
     * @return false when the failure was discarded because the step already left {@code RUNNING}
     */
    private boolean failStep(UUID workflowId, int position, ActivityResult result) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        WorkflowStepEntity step = stepAt(instance, position);
        if (!stillRunning(instance, step)) {
            return false;
        }
        step.setStatus(StepStatus.FAILED);
        step.setError(result.error());
        if (result.outputJson() != null) {
            step.setOutputJson(result.outputJson());
        }
        stampCompletedAt(step);
        history.append(instance, HistoryEventType.STEP_FAILED, step, null);
        if (instance.getStatus() == WorkflowStatus.RUNNING && planCompensation(instance, step)) {
            instance.setStatus(WorkflowStatus.COMPENSATING);
            instance.setCurrentStep(step.getName());
            instance.setError(result.error());
            instance.setOutputJson(null);
            history.append(instance, HistoryEventType.WORKFLOW_COMPENSATING, null);
            log.info("workflow compensating workflowId={} type={} step={} attempt={} status=COMPENSATING version={}",
                    workflowId, instance.getType(), step.getName(), step.getAttempt(), instance.getVersion());
            return true;
        }
        instance.setStatus(WorkflowStatus.FAILED);
        instance.setCurrentStep(step.getName());
        instance.setError(result.error());
        instance.setOutputJson(null);
        history.append(instance, HistoryEventType.WORKFLOW_FAILED, null);

        log.info("workflow failed workflowId={} type={} step={} attempt={} status=FAILED version={}",
                workflowId, instance.getType(), step.getName(), step.getAttempt(), instance.getVersion());
        return true;
    }

    /**
     * Applies {@link #TIMED_OUT_ERROR} when this instance still has a due
     * {@code RUNNING} step. No-op otherwise. Runs inside {@link #timeoutIfDue}'s
     * transaction.
     *
     * @param workflowId instance id
     */
    private void applyTimeoutIfDue(UUID workflowId) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElse(null);
        if (instance == null || !forwardOrCompensating(instance.getStatus())) {
            return;
        }
        Instant now = clock.instant();
        for (WorkflowStepEntity step : instance.getSteps()) {
            if (step.getStatus() == StepStatus.RUNNING && deadlineDue(step.getDeadlineAt(), now)) {
                failStep(workflowId, step.getPosition(), new ActivityResult(false, null, TIMED_OUT_ERROR));
                return;
            }
        }
    }

    /**
     * Whether a completion or failure write is still allowed.
     *
     * @param instance loaded aggregate
     * @param step step the executor intended to finish
     * @return true only when both are still {@code RUNNING}
     */
    private boolean stillRunning(WorkflowInstanceEntity instance, WorkflowStepEntity step) {
        if (step.getStatus() == StepStatus.RUNNING && forwardOrCompensating(instance.getStatus())) {
            return true;
        }
        log.info("step write skipped workflowId={} type={} step={} attempt={} status={} version={} reason=not_running",
                instance.getId(), instance.getType(), step.getName(), step.getAttempt(),
                step.getStatus(), instance.getVersion());
        return false;
    }

    /**
     * End of this attempt on the engine clock. Null when the definition has no timeout.
     *
     * @param now clock instant at step-start or running-resume
     * @param timeout per-attempt limit; null means no deadline
     * @return deadline to store, or null
     */
    private static Instant deadlineFor(Instant now, Duration timeout) {
        if (timeout == null) {
            return null;
        }
        return now.plus(timeout);
    }

    /**
     * Sets {@code started_at} from PostgreSQL {@code now()}, not the JVM clock.
     *
     * @param step row already in the persistence context
     */
    private void stampStartedAt(WorkflowStepEntity step) {
        stampTimestamp(step, "update workflow_step set started_at = now() where id = :id");
    }

    /**
     * Sets {@code completed_at} from PostgreSQL {@code now()}.
     *
     * @param step row already in the persistence context
     */
    private void stampCompletedAt(WorkflowStepEntity step) {
        stampTimestamp(step, "update workflow_step set completed_at = now() where id = :id");
    }

    /**
     * Native update plus refresh so the entity matches the database clock.
     *
     * @param step step to stamp
     * @param sql parameterized update using {@code :id}
     */
    private void stampTimestamp(WorkflowStepEntity step, String sql) {
        entityManager.flush();
        entityManager.createNativeQuery(sql)
                .setParameter("id", step.getId())
                .executeUpdate();
        entityManager.refresh(step);
    }

    /**
     * Resolves a step by {@code position}, not by name.
     *
     * @param instance aggregate with steps loaded
     * @param position zero-based index
     * @return matching step
     * @throws IllegalStateException if the position is missing
     */
    /**
     * Inserts pending compensation rows for completed forward steps before
     * {@code failed}, newest first. A step with no compensator is skipped.
     *
     * @param instance aggregate already in this transaction
     * @param failed forward step that just failed
     * @return true when at least one compensation row was inserted
     */
    private boolean planCompensation(WorkflowInstanceEntity instance, WorkflowStepEntity failed) {
        WorkflowDefinition definition = definitions.findByType(instance.getType()).orElse(null);
        if (definition == null) {
            return false;
        }
        List<WorkflowStepEntity> completed = instance.getSteps().stream()
                .filter(step -> step.getPosition() < failed.getPosition()
                        && step.getStatus() == StepStatus.COMPLETED)
                .sorted(java.util.Comparator.comparingInt(WorkflowStepEntity::getPosition).reversed())
                .toList();
        int nextPosition = instance.getSteps().stream()
                .mapToInt(WorkflowStepEntity::getPosition)
                .max()
                .orElse(-1) + 1;
        boolean planned = false;
        for (WorkflowStepEntity forward : completed) {
            String compensationName = compensationName(definition, forward.getName());
            if (compensationName == null) {
                continue;
            }
            WorkflowStepEntity compensation = new WorkflowStepEntity();
            compensation.setId(java.util.UUID.randomUUID());
            compensation.setName(compensationName);
            compensation.setPosition(nextPosition++);
            compensation.setStatus(StepStatus.PENDING);
            compensation.setAttempt(0);
            instance.addStep(compensation);
            entityManager.persist(compensation);
            planned = true;
            history.append(instance, HistoryEventType.COMPENSATION_PLANNED, compensation, null);
        }
        return planned;
    }

    /**
     * Records {@code FORWARD_COMPENSATED} when {@code compensationName} undoes a forward step.
     *
     * @param instance aggregate in this transaction
     * @param compensationName activity that just succeeded
     */
    private void appendForwardCompensated(WorkflowInstanceEntity instance, String compensationName) {
        WorkflowStepEntity forward = markForwardCompensated(instance, compensationName);
        if (forward != null) {
            history.append(instance, HistoryEventType.FORWARD_COMPENSATED, forward, null);
        }
    }

    /**
     * Sets the forward step that {@code compensationName} undoes to {@code COMPENSATED}.
     *
     * @param instance aggregate in this transaction
     * @param compensationName activity that just succeeded
     * @return the forward step that changed, or null when this activity is not a compensator
     */
    private WorkflowStepEntity markForwardCompensated(WorkflowInstanceEntity instance, String compensationName) {
        WorkflowDefinition definition = definitions.findByType(instance.getType()).orElse(null);
        if (definition == null || !isCompensation(definition, compensationName)) {
            return null;
        }
        String forwardName = definition.steps().stream()
                .filter(step -> compensationName.equals(step.compensationName()))
                .map(com.workflowengine.domain.StepDefinition::name)
                .findFirst()
                .orElse(null);
        if (forwardName == null) {
            return null;
        }
        return instance.getSteps().stream()
                .filter(step -> forwardName.equals(step.getName()))
                .findFirst()
                .map(step -> {
                    step.setStatus(StepStatus.COMPENSATED);
                    return step;
                })
                .orElse(null);
    }

    private static boolean isCompensation(WorkflowDefinition definition, String stepName) {
        return compensationName(definition, stepName) == null
                && definition.steps().stream().anyMatch(step -> stepName.equals(step.compensationName()));
    }

    private static String compensationName(WorkflowDefinition definition, String forwardName) {
        return definition.steps().stream()
                .filter(step -> step.name().equals(forwardName))
                .map(com.workflowengine.domain.StepDefinition::compensationName)
                .findFirst()
                .orElse(null);
    }

    private static Duration timeoutFor(WorkflowDefinition definition, String stepName) {
        for (com.workflowengine.domain.StepDefinition step : definition.steps()) {
            if (step.name().equals(stepName) || stepName.equals(step.compensationName())) {
                return step.timeout();
            }
        }
        return null;
    }

    private static boolean forwardOrCompensating(WorkflowStatus status) {
        return status == WorkflowStatus.RUNNING || status == WorkflowStatus.COMPENSATING;
    }

    private boolean compensating(UUID workflowId) {
        return instances.findById(workflowId)
                .map(instance -> instance.getStatus() == WorkflowStatus.COMPENSATING)
                .orElse(false);
    }

    private static WorkflowStepEntity stepAt(WorkflowInstanceEntity instance, int position) {
        return instance.getSteps().stream()
                .filter(step -> step.getPosition() == position)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("missing step at position " + position));
    }
}
