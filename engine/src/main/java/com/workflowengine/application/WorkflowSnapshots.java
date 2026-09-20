package com.workflowengine.application;

import com.workflowengine.api.StepSnapshot;
import com.workflowengine.api.WorkflowSnapshot;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowStepEntity;

import java.util.Comparator;
import java.util.List;

/**
 * Maps JPA aggregates to {@link WorkflowSnapshot}. Steps are sorted by
 * {@code position}. Null {@code version} is treated as 0.
 *
 * <p>Pure functions; no persistence. Safe to call from request or executor
 * threads after the entity is loaded.
 */
public final class WorkflowSnapshots {

    private WorkflowSnapshots() {
    }

    /**
     * Copies instance and steps into an immutable snapshot.
     *
     * @param instance loaded aggregate; steps must be present
     * @return snapshot with steps ordered by position
     */
    public static WorkflowSnapshot from(WorkflowInstanceEntity instance) {
        List<StepSnapshot> steps = instance.getSteps().stream()
                .sorted(Comparator.comparingInt(WorkflowStepEntity::getPosition))
                .map(WorkflowSnapshots::from)
                .toList();
        int version = instance.getVersion() == null ? 0 : instance.getVersion();
        return new WorkflowSnapshot(
                instance.getId(),
                instance.getType(),
                instance.getDefinitionVersion(),
                instance.getStatus(),
                instance.getCurrentStep(),
                instance.getIdempotencyKey(),
                version,
                instance.getInputJson(),
                instance.getOutputJson(),
                instance.getError(),
                instance.getCreatedAt(),
                instance.getUpdatedAt(),
                steps
        );
    }

    /**
     * Copies one step. {@code input_json} is omitted from the public snapshot.
     *
     * @param step loaded step
     * @return step snapshot
     */
    private static StepSnapshot from(WorkflowStepEntity step) {
        return new StepSnapshot(
                step.getName(),
                step.getPosition(),
                step.getStatus(),
                step.getAttempt(),
                step.getOutputJson(),
                step.getError(),
                step.getStartedAt(),
                step.getCompletedAt()
        );
    }
}
