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

/**
 * Persists a new ORDER instance and submits in-process execution after commit.
 *
 * <p>This is the Phase 1 admission service: it is the only writer that creates
 * rows, and the only place that may submit {@link WorkflowExecutor}. HTTP
 * adapters in {@code com.workflowengine.web} call this type; they do not talk
 * to the executor or repositories directly.
 *
 * <p><strong>Admit-then-run</strong> and <strong>idempotent admission</strong>:
 * the admit transaction inserts the instance as {@code PENDING}, {@code version = 0},
 * {@code current_step} null, and every step {@code PENDING}. The returned
 * snapshot is that admit aggregate. Callers must not reload it after submit.
 * Concurrent posts with the same key serialize on
 * {@code uq_workflow_instance_idempotency_key}. Only the INSERT winner
 * submits the executor. The unique-violation path returns the existing
 * snapshot and does not submit.
 *
 * <p>Concurrency: many request threads may call {@link #start} at once. This
 * type is a Spring singleton; it holds no per-request state.
 *
 * <p>Failure handling: validation throws {@link InvalidStartWorkflowException}
 * (HTTP 400). Unique-key conflict reloads the existing row and returns
 * {@code created = false}. Executor-thread failures after a successful admit
 * are not reported on this call; clients poll GET.
 */
@Slf4j
@Service
public class StartWorkflowService {

    private final WorkflowInstanceRepository instances;
    private final WorkflowDefinitionRegistry definitions;
    private final WorkflowExecutor workflowExecutor;
    private final TaskExecutor workflowTaskExecutor;
    private final TransactionTemplate transactionTemplate;

    /**
     * Wires admission. The transaction template is built here so the admit transaction is
     * explicit and not a class-level {@code @Transactional} around invoke.
     *
     * @param instances instance repository
     * @param definitions type registry
     * @param workflowExecutor runner submitted after the admit transaction
     * @param workflowTaskExecutor pool that must not be the request thread
     * @param transactionManager used only for admit insert
     */
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

    /**
     * Admits a workflow on the HTTP request thread.
     *
     * @param command validated type, key, and input JSON; must not be null
     * @return admit snapshot when {@code created} is true; current snapshot otherwise
     * @throws InvalidStartWorkflowException when type, key, or input is invalid
     * @apiNote {@code 201} is not terminal. Poll GET.
     */
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

    /**
     * Rejects unknown type, missing/blank/too-long key, and blank input.
     *
     * @param command admit command
     * @return registered definition
     * @throws InvalidStartWorkflowException on any validation miss
     */
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

    /**
     * Admit transaction: insert instance {@code PENDING} plus one {@code PENDING} step per
     * definition position. Flushes so unique-key races surface here.
     *
     * @param command admit command
     * @param definition graph whose version and steps are copied
     * @return persisted aggregate still at version 0
     */
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

    /**
     * Distinguishes the idempotency unique index from other integrity errors.
     *
     * @param ex Spring data-integrity wrapper
     * @return true only for {@code uq_workflow_instance_idempotency_key}
     */
    private static boolean isIdempotencyKeyViolation(DataIntegrityViolationException ex) {
        Throwable cause = ex.getMostSpecificCause();
        String message = cause.getMessage();
        return message != null && message.contains("uq_workflow_instance_idempotency_key");
    }
}
