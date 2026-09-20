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

@Slf4j
@Component
public class WorkflowExecutor {

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
        instance.setStatus(WorkflowStatus.RUNNING);
        instance.setCurrentStep(stepName);
        stampStartedAt(step);

        log.info("step started workflowId={} type={} step={} attempt={} status=RUNNING version={}",
                workflowId, instance.getType(), stepName, attempt, instance.getVersion());
        return attempt;
    }

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

    private void stampStartedAt(WorkflowStepEntity step) {
        stampTimestamp(step, "update workflow_step set started_at = now() where id = :id");
    }

    private void stampCompletedAt(WorkflowStepEntity step) {
        stampTimestamp(step, "update workflow_step set completed_at = now() where id = :id");
    }

    private void stampTimestamp(WorkflowStepEntity step, String sql) {
        entityManager.flush();
        entityManager.createNativeQuery(sql)
                .setParameter("id", step.getId())
                .executeUpdate();
        entityManager.refresh(step);
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
