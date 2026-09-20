package com.workflowengine.runtime;

import com.workflowengine.activity.ActivityInvoker;
import com.workflowengine.api.StepStatus;
import com.workflowengine.api.WorkflowStatus;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;
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

import java.util.List;
import java.util.UUID;

/**
 * Runs a linear ORDER saga with commit-before-invoke.
 *
 * <p>Each unit of work is its own committed transaction. {@link ActivityInvoker#invoke}
 * runs with no open workflow TX. Instance {@code PENDING→RUNNING} is folded
 * into the first step-start TX. Last-step {@code COMPLETED} and instance
 * {@code COMPLETED} share one TX so a leftover of instance {@code RUNNING}
 * plus all steps {@code COMPLETED} cannot exist.
 *
 * <p><strong>Leftovers:</strong> a crash after TX1 before the first start TX
 * leaves instance {@code PENDING}. A crash during invoke after the start TX
 * leaves a {@code RUNNING} step. Phase 1 does not resume either. {@code FAILED}
 * is terminal.
 *
 * <p>Runs on {@code workflow-*} threads, never the HTTP request thread. This
 * class must not import {@code activity.stub}. Infrastructure failures are
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

    /**
     * Builds an executor that opens its own {@link TransactionTemplate} per unit of work.
     *
     * @param instances instance repository
     * @param definitions type registry
     * @param invoker sync activity port
     * @param transactionManager not used as a class-level {@code @Transactional}
     * @param entityManager used to force {@code @Version} bumps and stamp timestamps
     */
    public WorkflowExecutor(
            WorkflowInstanceRepository instances,
            WorkflowDefinitionRegistry definitions,
            ActivityInvoker invoker,
            PlatformTransactionManager transactionManager,
            EntityManager entityManager
    ) {
        this.instances = instances;
        this.definitions = definitions;
        this.invoker = invoker;
        this.tx = new TransactionTemplate(transactionManager);
        this.entityManager = entityManager;
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
     * Walks definition steps: start TX, invoke, complete or fail TX.
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
            Integer attempt = tx.execute(status -> startStep(workflowId, position, stepDef.name()));
            if (attempt == null) {
                return;
            }

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
     * Start TX: step {@code PENDING→RUNNING}, increment {@code attempt}, set
     * instance {@code RUNNING} and {@code current_step}, stamp {@code started_at}.
     *
     * @param workflowId instance id
     * @param position step index
     * @param stepName definition name
     * @return attempt after increment, or null if the instance is already terminal
     *         or the step is not {@code PENDING}
     */
    private Integer startStep(UUID workflowId, int position, String stepName) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        if (instance.getStatus() == WorkflowStatus.COMPLETED
                || instance.getStatus() == WorkflowStatus.FAILED) {
            return null;
        }
        WorkflowStepEntity step = stepAt(instance, position);
        if (step.getStatus() != StepStatus.PENDING) {
            return null;
        }

        int attempt = step.getAttempt() + 1;
        step.setStatus(StepStatus.RUNNING);
        step.setAttempt(attempt);
        instance.setStatus(WorkflowStatus.RUNNING);
        instance.setCurrentStep(stepName);
        stampStartedAt(step);

        log.info("step started workflowId={} type={} step={} attempt={} status=RUNNING version={}",
                workflowId, instance.getType(), stepName, attempt, instance.getVersion());
        return attempt;
    }

    /**
     * Mid-step success TX: step {@code COMPLETED}, force {@code @Version} bump
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
     * Last-step success TX: step and instance {@code COMPLETED}, copy last
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
     * Failure TX: step and instance {@code FAILED}. Instance {@code output_json}
     * stays null. Terminal until a retry policy exists.
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
}
