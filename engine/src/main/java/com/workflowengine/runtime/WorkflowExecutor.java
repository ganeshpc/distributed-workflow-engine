package com.workflowengine.runtime;

import com.workflowengine.activity.ActivityInvoker;
import com.workflowengine.api.StepStatus;
import com.workflowengine.api.WorkflowStatus;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;
import com.workflowengine.domain.RetryPolicy;
import com.workflowengine.domain.StepDefinition;
import com.workflowengine.domain.WorkflowDefinition;
import com.workflowengine.domain.WorkflowDefinitionRegistry;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import com.workflowengine.persistence.WorkflowStepEntity;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Runs a linear ORDER saga with commit-before-invoke.
 *
 * <p>Each unit of work is its own committed transaction. {@link ActivityInvoker#invoke}
 * runs with no open workflow transaction. Instance {@code PENDING→RUNNING} is folded
 * into the first step-start transaction. Last-step {@code COMPLETED} and instance
 * {@code COMPLETED} share one workflow-complete transaction so a leftover of instance {@code RUNNING}
 * plus all steps {@code COMPLETED} cannot exist.
 *
 * <p><strong>Leftovers:</strong> Phase 2 resumes never-started {@code PENDING} and due
 * {@code RUNNING} steps (attempt++) while {@code deadline_at} is still in the
 * future. {@code FAILED} stays terminal. A leftover {@code RUNNING} whose next
 * attempt would exceed {@link RetryPolicy#maxAttempts()} becomes poison
 * {@code RETRY_EXHAUSTED}. A due {@code deadline_at} becomes {@code FAILED}
 * with {@link #TIMED_OUT_ERROR} and is not re-invoked.
 *
 * <p>Runs on {@code workflow-*} threads, never the HTTP request thread.
 * {@link #timeoutIfDue} also runs on the scheduler thread. This class must not
 * import the in-process stub package. Infrastructure failures are logged and
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
    private final ActivityInvoker invoker;
    private final TransactionTemplate tx;
    private final EntityManager entityManager;
    private final Clock clock;

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
     * @param invoker sync activity port
     * @param transactionManager not used as a class-level {@code @Transactional}
     * @param entityManager used to force {@code @Version} bumps and stamp timestamps
     * @param clock backoff due-time and step-deadline clock for {@code RUNNING} steps
     */
    public WorkflowExecutor(
            WorkflowInstanceRepository instances,
            WorkflowDefinitionRegistry definitions,
            ActivityInvoker invoker,
            PlatformTransactionManager transactionManager,
            EntityManager entityManager,
            Clock clock
    ) {
        this.instances = instances;
        this.definitions = definitions;
        this.invoker = invoker;
        this.tx = new TransactionTemplate(transactionManager);
        this.entityManager = entityManager;
        this.clock = clock;
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
     * Walks definition steps: step-start, invoke, complete or fail.
     *
     * @param workflowId instance id
     */
    private void execute(UUID workflowId) {
        Seed seed = tx.execute(status -> loadSeed(workflowId));
        if (seed == null) {
            log.error("executor infrastructure failure workflowId={} reason=not_found", workflowId);
            return;
        }
        WorkflowDefinition definition = definitions.findByType(seed.type()).orElse(null);
        if (definition == null) {
            log.error("executor infrastructure failure workflowId={} reason=unknown_type type={}",
                    workflowId, seed.type());
            return;
        }

        List<StepDefinition> steps = definition.steps();
        for (int i = 0; i < steps.size(); i++) {
            int position = i;
            StepDefinition stepDef = steps.get(position);
            Prepared prepared = tx.execute(status -> prepareStep(workflowId, position, stepDef));
            if (prepared == null || prepared.kind() == Prepared.Kind.STOP) {
                return;
            }
            if (prepared.kind() == Prepared.Kind.SKIP) {
                continue;
            }
            int attempt = prepared.attempt();

            ActivityResult result = invoker.invoke(new ActivityContext(
                    workflowId,
                    seed.type(),
                    seed.definitionVersion(),
                    stepDef.name(),
                    attempt,
                    seed.inputJson(),
                    null
            ));

            boolean last = position == steps.size() - 1;
            Boolean wrote = tx.execute(status -> {
                if (!result.success()) {
                    return failStep(workflowId, position, result);
                }
                if (last) {
                    return completeLastStep(workflowId, position, result);
                }
                return completeMidStep(workflowId, position, result);
            });
            if (!result.success() || !Boolean.TRUE.equals(wrote)) {
                return;
            }
        }
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
            }
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
     * Reads type, definition version, and input once. Later steps reuse this
     * so every activity sees the same workflow input JSON.
     *
     * @param workflowId instance id
     * @return seed, or null if the row is missing
     */
    private Seed loadSeed(UUID workflowId) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElse(null);
        if (instance == null) {
            return null;
        }
        return new Seed(instance.getType(), instance.getDefinitionVersion(), instance.getInputJson());
    }

    /**
     * Chooses skip (already {@code COMPLETED}), stop (terminal, not due, timed out, or poison),
     * or run (step-start / leftover resume).
     *
     * @param workflowId instance id
     * @param position step index
     * @param stepDef name, retry policy, and per-attempt timeout
     * @return prepared action; never null
     */
    private Prepared prepareStep(UUID workflowId, int position, StepDefinition stepDef) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        if (instance.getStatus() == WorkflowStatus.COMPLETED
                || instance.getStatus() == WorkflowStatus.FAILED) {
            return Prepared.stop();
        }
        WorkflowStepEntity step = stepAt(instance, position);
        if (step.getStatus() == StepStatus.COMPLETED) {
            return Prepared.skip();
        }
        if (step.getStatus() == StepStatus.FAILED) {
            return Prepared.stop();
        }
        if (step.getStatus() == StepStatus.PENDING) {
            return Prepared.run(startStep(instance, step, stepDef.name(), stepDef.timeout()));
        }
        return resumeRunning(instance, step, stepDef.retryPolicy(), stepDef.timeout());
    }

    /**
     * Step-start transaction: step {@code PENDING→RUNNING}, increment {@code attempt}, set
     * instance {@code RUNNING} and {@code current_step}, stamp {@code started_at}
     * from the database and {@code deadline_at} from the engine clock.
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
        step.setStatus(StepStatus.RUNNING);
        step.setAttempt(attempt);
        step.setNextAttemptAt(null);
        step.setDeadlineAt(deadlineFor(clock.instant(), timeout));
        instance.setStatus(WorkflowStatus.RUNNING);
        instance.setCurrentStep(stepName);
        stampStartedAt(step);

        log.info("step started workflowId={} type={} step={} attempt={} status=RUNNING version={}",
                instance.getId(), instance.getType(), stepName, attempt, instance.getVersion());
        return attempt;
    }

    /**
     * Resume leftover {@code RUNNING}, or fail it when the deadline is due.
     * A due deadline wins over backoff and over {@code RETRY_EXHAUSTED}:
     * the attempt is not incremented and the activity is not invoked.
     * Otherwise: not due yet → stop; next attempt above {@code maxAttempts}
     * → poison {@code FAILED}; else increment {@code attempt}, refresh
     * {@code deadline_at}, then invoke again.
     *
     * @param instance loaded aggregate
     * @param step the {@code RUNNING} step
     * @param retryPolicy attempt cap and backoff
     * @param timeout per-attempt limit applied to the new attempt; null means no deadline
     * @return run with the new attempt, or stop
     */
    private Prepared resumeRunning(
            WorkflowInstanceEntity instance,
            WorkflowStepEntity step,
            RetryPolicy retryPolicy,
            Duration timeout
    ) {
        Instant now = clock.instant();
        if (deadlineDue(step.getDeadlineAt(), now)) {
            failStep(instance.getId(), step.getPosition(),
                    new ActivityResult(false, null, TIMED_OUT_ERROR));
            return Prepared.stop();
        }
        Instant next = step.getNextAttemptAt();
        if (next != null && next.isAfter(now)) {
            return Prepared.stop();
        }
        int attempt = step.getAttempt() + 1;
        if (attempt > retryPolicy.maxAttempts()) {
            failStep(instance.getId(), step.getPosition(),
                    new ActivityResult(false, null, "RETRY_EXHAUSTED"));
            return Prepared.stop();
        }
        step.setAttempt(attempt);
        step.setNextAttemptAt(retryPolicy.initialBackoff().isZero()
                ? null
                : now.plus(retryPolicy.initialBackoff()));
        step.setDeadlineAt(deadlineFor(now, timeout));
        entityManager.lock(instance, LockModeType.OPTIMISTIC_FORCE_INCREMENT);

        log.info("step resumed workflowId={} type={} step={} attempt={} status=RUNNING version={}",
                instance.getId(), instance.getType(), step.getName(), attempt, instance.getVersion());
        return Prepared.run(attempt);
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

        log.info("workflow completed workflowId={} type={} step={} attempt={} status=COMPLETED version={}",
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
        instance.setStatus(WorkflowStatus.FAILED);
        instance.setCurrentStep(step.getName());
        instance.setError(result.error());
        instance.setOutputJson(null);
        stampCompletedAt(step);

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
        if (instance == null || instance.getStatus() != WorkflowStatus.RUNNING) {
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
        if (step.getStatus() == StepStatus.RUNNING && instance.getStatus() == WorkflowStatus.RUNNING) {
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
    private static WorkflowStepEntity stepAt(WorkflowInstanceEntity instance, int position) {
        return instance.getSteps().stream()
                .filter(step -> step.getPosition() == position)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("missing step at position " + position));
    }

    /**
     * Values reused for every {@link ActivityContext} so step input is not chained.
     *
     * @param type workflow type
     * @param definitionVersion version stored at admit
     * @param inputJson workflow input JSON text
     */
    private record Seed(String type, int definitionVersion, String inputJson) {
    }

    /**
     * Result of {@link #prepareStep}. {@code SKIP} continues the definition walk;
     * {@code STOP} ends the run; {@code RUN} invokes with {@code attempt}.
     *
     * @param kind what the executor should do next
     * @param attempt current attempt when {@code kind} is {@code RUN}; otherwise null
     */
    private record Prepared(Kind kind, Integer attempt) {

        enum Kind {
            STOP,
            SKIP,
            RUN
        }

        static Prepared stop() {
            return new Prepared(Kind.STOP, null);
        }

        static Prepared skip() {
            return new Prepared(Kind.SKIP, null);
        }

        static Prepared run(int attempt) {
            return new Prepared(Kind.RUN, attempt);
        }
    }
}
