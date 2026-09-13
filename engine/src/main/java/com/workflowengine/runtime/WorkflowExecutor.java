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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
public class WorkflowExecutor {

    private static final Logger log = LoggerFactory.getLogger(WorkflowExecutor.class);

    private final WorkflowInstanceRepository instances;
    private final WorkflowDefinitionRegistry definitions;
    private final ActivityInvoker invoker;
    private final TransactionTemplate tx;
    private final EntityManager entityManager;

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

    public void run(UUID workflowId) {
        try {
            execute(workflowId);
        } catch (RuntimeException ex) {
            log.error("executor infrastructure failure workflowId={}", workflowId, ex);
        }
    }

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

    private Seed loadSeed(UUID workflowId) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElse(null);
        if (instance == null) {
            return null;
        }
        return new Seed(instance.getType(), instance.getDefinitionVersion(), instance.getInputJson());
    }

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
        step.setStartedAt(Instant.now());
        instance.setStatus(WorkflowStatus.RUNNING);
        instance.setCurrentStep(stepName);

        log.info("step started workflowId={} type={} step={} attempt={} status=RUNNING version={}",
                workflowId, instance.getType(), stepName, attempt, instance.getVersion() + 1);
        return attempt;
    }

    private void completeMidStep(UUID workflowId, int position, ActivityResult result) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        WorkflowStepEntity step = stepAt(instance, position);
        step.setStatus(StepStatus.COMPLETED);
        step.setOutputJson(result.outputJson());
        step.setError(null);
        step.setCompletedAt(Instant.now());
        entityManager.lock(instance, LockModeType.OPTIMISTIC_FORCE_INCREMENT);

        log.info("step completed workflowId={} type={} step={} attempt={} status=COMPLETED version={}",
                workflowId, instance.getType(), step.getName(), step.getAttempt(), instance.getVersion() + 1);
    }

    private void completeLastStep(UUID workflowId, int position, ActivityResult result) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        WorkflowStepEntity step = stepAt(instance, position);
        step.setStatus(StepStatus.COMPLETED);
        step.setOutputJson(result.outputJson());
        step.setError(null);
        step.setCompletedAt(Instant.now());
        instance.setStatus(WorkflowStatus.COMPLETED);
        instance.setCurrentStep(step.getName());
        instance.setOutputJson(result.outputJson());
        instance.setError(null);

        log.info("workflow completed workflowId={} type={} step={} attempt={} status=COMPLETED version={}",
                workflowId, instance.getType(), step.getName(), step.getAttempt(), instance.getVersion() + 1);
    }

    private void failStep(UUID workflowId, int position, ActivityResult result) {
        WorkflowInstanceEntity instance = instances.findById(workflowId).orElseThrow();
        WorkflowStepEntity step = stepAt(instance, position);
        step.setStatus(StepStatus.FAILED);
        step.setError(result.error());
        step.setCompletedAt(Instant.now());
        if (result.outputJson() != null) {
            step.setOutputJson(result.outputJson());
        }
        instance.setStatus(WorkflowStatus.FAILED);
        instance.setCurrentStep(step.getName());
        instance.setError(result.error());
        instance.setOutputJson(null);

        log.info("workflow failed workflowId={} type={} step={} attempt={} status=FAILED version={}",
                workflowId, instance.getType(), step.getName(), step.getAttempt(), instance.getVersion() + 1);
    }

    private static WorkflowStepEntity stepAt(WorkflowInstanceEntity instance, int position) {
        return instance.getSteps().stream()
                .filter(step -> step.getPosition() == position)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("missing step at position " + position));
    }

    private record Seed(String type, int definitionVersion, String inputJson) {
    }
}
