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
 * {@code RUNNING} steps (attempt++). {@code FAILED} stays terminal. A leftover
 * {@code RUNNING} whose next attempt would exceed {@link RetryPolicy#maxAttempts()}
 * becomes poison {@code RETRY_EXHAUSTED}.
 *
 * <p>Runs on {@code workflow-*} threads, never the HTTP request thread. This
 * class must not import the in-process stub package. Infrastructure failures are
 * logged and stop the run; they do not mark the step {@code FAILED}.
 *
 * @implNote Happy-path terminal {@code version == 10}. Mid-step success TXs
 * bump {@code @Version} with {@link LockModeType#OPTIMISTIC_FORCE_INCREMENT}.
 */
@Slf4j
@Component
public class WorkflowExecutor {

    private final WorkflowInstanceRepository instances;
    private final WorkflowDefinitionRegistry definitions;
    private final ActivityInvoker invoker;
    private final TransactionTemplate tx;
    private final EntityManager entityManager;
    private final Clock clock;

    /**
     * Builds an executor that opens its own {@link TransactionTemplate} per unit of work.
     *
     * @param instances instance repository
     * @param definitions type registry
     * @param invoker sync activity port
     * @param transactionManager not used as a class-level {@code @Transactional}
     * @param entityManager used to force {@code @Version} bumps and stamp timestamps
     * @param clock backoff due-time for leftover {@code RUNNING} steps
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
            if (!result.success()) {
                tx.executeWithoutResult(status -> failStep(workflowId, position, result));
                return;
            }
            if (last) {
                tx.executeWithoutResult(status -> completeLastStep(workflowId, position, result));
            } else {
                tx.executeWithoutResult(status -> completeMidStep(workflowId, position, result));
            }
        }
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
     * Chooses skip (already {@code COMPLETED}), stop (terminal, not due, or poison),
     * or run (step-start / leftover resume).
     *
     * @param workflowId instance id
     * @param position step index
     * @param stepDef name and retry policy
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
            return Prepared.run(startStep(instance, step, stepDef.name()));
        }
        return resumeRunning(instance, step, stepDef.retryPolicy());
    }

    /**
     * Step-start transaction: step {@code PENDING→RUNNING}, increment {@code attempt}, set
     * instance {@code RUNNING} and {@code current_step}, stamp {@code started_at}.
     *
     * @param instance loaded aggregate
     * @param step {@code PENDING} step
     * @param stepName definition name
     * @return attempt after increment
     */
    private int startStep(WorkflowInstanceEntity instance, WorkflowStepEntity step, String stepName) {
        int attempt = step.getAttempt() + 1;
        step.setStatus(StepStatus.RUNNING);
        step.setAttempt(attempt);
        step.setNextAttemptAt(null);
        instance.setStatus(WorkflowStatus.RUNNING);
        instance.setCurrentStep(stepName);
        stampStartedAt(step);

        log.info("step started workflowId={} type={} step={} attempt={} status=RUNNING version={}",
                instance.getId(), instance.getType(), stepName, attempt, instance.getVersion());
        return attempt;
    }

    /**
     * Resume leftover {@code RUNNING}: increment {@code attempt}, then invoke
     * again. Not due yet → stop. Next attempt above {@code maxAttempts} →
     * poison {@code FAILED}.
     *
     * @param instance loaded aggregate
     * @param step the {@code RUNNING} step
     * @param retryPolicy attempt cap and backoff
     * @return run with the new attempt, or stop
     */
    private Prepared resumeRunning(
            WorkflowInstanceEntity instance,
            WorkflowStepEntity step,
            RetryPolicy retryPolicy
    ) {
        Instant now = clock.instant();
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
     */
    private void completeMidStep(UUID workflowId, int position, ActivityResult result) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        WorkflowStepEntity step = stepAt(instance, position);
        step.setStatus(StepStatus.COMPLETED);
        step.setOutputJson(result.outputJson());
        step.setError(null);
        entityManager.lock(instance, LockModeType.OPTIMISTIC_FORCE_INCREMENT);
        stampCompletedAt(step);

        log.info("step completed workflowId={} type={} step={} attempt={} status=COMPLETED version={}",
                workflowId, instance.getType(), step.getName(), step.getAttempt(), instance.getVersion());
    }

    /**
     * Workflow-complete transaction: step and instance {@code COMPLETED}, copy last
     * output onto the instance.
     *
     * @param workflowId instance id
     * @param position last step index
     * @param result successful activity output
     */
    private void completeLastStep(UUID workflowId, int position, ActivityResult result) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        WorkflowStepEntity step = stepAt(instance, position);
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
    }

    /**
     * Step-fail transaction: step and instance {@code FAILED}. Instance {@code output_json}
     * stays null. Terminal; Phase 2 does not retry {@code FAILED}.
     *
     * @param workflowId instance id
     * @param position failed step index
     * @param result unsuccessful activity result
     */
    private void failStep(UUID workflowId, int position, ActivityResult result) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        WorkflowStepEntity step = stepAt(instance, position);
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
