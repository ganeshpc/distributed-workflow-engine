package com.workflowengine.application;

import com.workflowengine.api.StartWorkflowCommand;
import com.workflowengine.api.StepStatus;
import com.workflowengine.api.WorkflowStatus;
import com.workflowengine.domain.StepDefinition;
import com.workflowengine.domain.WorkflowDefinition;
import com.workflowengine.domain.WorkflowDefinitionRegistry;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import com.workflowengine.persistence.WorkflowStepEntity;
import com.workflowengine.runtime.WorkflowExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class StartWorkflowService {

    private final WorkflowInstanceRepository instances;
    private final WorkflowDefinitionRegistry definitions;
    private final WorkflowExecutor workflowExecutor;
    private final TaskExecutor workflowTaskExecutor;
    private final TransactionTemplate transactionTemplate;

    public StartWorkflowService(
            WorkflowInstanceRepository instances,
            WorkflowDefinitionRegistry definitions,
            WorkflowExecutor workflowExecutor,
            @Qualifier("workflowTaskExecutor") TaskExecutor workflowTaskExecutor,
            PlatformTransactionManager transactionManager
    ) {
        this.instances = instances;
        this.definitions = definitions;
        this.workflowExecutor = workflowExecutor;
        this.workflowTaskExecutor = workflowTaskExecutor;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public AdmissionResult start(StartWorkflowCommand command) {
        WorkflowDefinition definition = validate(command);

        WorkflowInstanceEntity created;
        try {
            created = transactionTemplate.execute(status -> insert(command, definition));
        } catch (DataIntegrityViolationException ex) {
            if (!isIdempotencyKeyViolation(ex)) {
                throw ex;
            }
            WorkflowInstanceEntity existing = instances.findByIdempotencyKey(command.idempotencyKey())
                    .orElseThrow(() -> new IllegalStateException(
                            "idempotency key not found after unique violation"));
            log.info("workflow admitted existing workflowId={} type={} status={} version={}",
                    existing.getId(), existing.getType(), existing.getStatus(), existing.getVersion());
            return new AdmissionResult(false, WorkflowSnapshots.from(existing));
        }

        UUID workflowId = created.getId();
        workflowTaskExecutor.execute(() -> workflowExecutor.run(workflowId));
        log.info("workflow admitted workflowId={} type={} status=PENDING version=0",
                workflowId, created.getType());
        return new AdmissionResult(true, WorkflowSnapshots.from(created));
    }

    private WorkflowDefinition validate(StartWorkflowCommand command) {
        if (command == null) {
            throw new InvalidStartWorkflowException("command is required");
        }
        if (command.type() == null || command.type().isBlank()) {
            throw new InvalidStartWorkflowException("type is required");
        }
        WorkflowDefinition definition = definitions.findByType(command.type())
                .orElseThrow(() -> new InvalidStartWorkflowException("unknown workflow type"));
        String key = command.idempotencyKey();
        if (key == null || key.isBlank() || key.length() > 128) {
            throw new InvalidStartWorkflowException("idempotencyKey is required (1..128 chars)");
        }
        if (command.inputJson() == null || command.inputJson().isBlank()) {
            throw new InvalidStartWorkflowException("inputJson is required");
        }
        return definition;
    }

    private WorkflowInstanceEntity insert(StartWorkflowCommand command, WorkflowDefinition definition) {
        WorkflowInstanceEntity instance = new WorkflowInstanceEntity();
        instance.setId(UUID.randomUUID());
        instance.setType(definition.type());
        instance.setDefinitionVersion(definition.version());
        instance.setStatus(WorkflowStatus.PENDING);
        instance.setInputJson(command.inputJson());
        instance.setIdempotencyKey(command.idempotencyKey());

        List<StepDefinition> steps = definition.steps();
        for (int i = 0; i < steps.size(); i++) {
            WorkflowStepEntity step = new WorkflowStepEntity();
            step.setId(UUID.randomUUID());
            step.setName(steps.get(i).name());
            step.setPosition(i);
            step.setStatus(StepStatus.PENDING);
            step.setAttempt(0);
            instance.addStep(step);
        }
        return instances.saveAndFlush(instance);
    }

    private static boolean isIdempotencyKeyViolation(DataIntegrityViolationException ex) {
        Throwable cause = ex.getMostSpecificCause();
        String message = cause.getMessage();
        return message != null && message.contains("uq_workflow_instance_idempotency_key");
    }
}
