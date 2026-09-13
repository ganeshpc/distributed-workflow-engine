package com.workflowengine.application;

import com.workflowengine.api.StepSnapshot;
import com.workflowengine.api.WorkflowSnapshot;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowStepEntity;

import java.util.Comparator;
import java.util.List;

public final class WorkflowSnapshots {

    private WorkflowSnapshots() {
    }

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
