package com.workflowengine.web;

import com.workflowengine.api.StepSnapshot;
import com.workflowengine.api.WorkflowSnapshot;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

final class WorkflowResponses {

    private WorkflowResponses() {
    }

    static WorkflowResponse from(WorkflowSnapshot snapshot, ObjectMapper mapper) {
        List<StepResponse> steps = snapshot.steps().stream()
                .map(step -> from(step, mapper))
                .toList();
        return new WorkflowResponse(
                snapshot.id(),
                snapshot.type(),
                snapshot.definitionVersion(),
                snapshot.status(),
                snapshot.currentStep(),
                snapshot.idempotencyKey(),
                snapshot.version(),
                parse(mapper, snapshot.inputJson()),
                parse(mapper, snapshot.outputJson()),
                snapshot.error(),
                snapshot.createdAt(),
                snapshot.updatedAt(),
                steps
        );
    }

    private static StepResponse from(StepSnapshot step, ObjectMapper mapper) {
        return new StepResponse(
                step.name(),
                step.position(),
                step.status(),
                step.attempt(),
                parse(mapper, step.outputJson()),
                step.error(),
                step.startedAt(),
                step.completedAt()
        );
    }

    private static JsonNode parse(ObjectMapper mapper, String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return mapper.readTree(json);
        } catch (JacksonException ex) {
            throw new IllegalStateException("stored json is not valid", ex);
        }
    }
}
