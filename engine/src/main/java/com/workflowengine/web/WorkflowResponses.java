package com.workflowengine.web;

import com.workflowengine.api.StepSnapshot;
import com.workflowengine.api.WorkflowSnapshot;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

/**
 * Maps {@link WorkflowSnapshot} JSON strings into Jackson trees for HTTP.
 *
 * <p>Invalid stored JSON is an illegal state (the engine wrote it), not a
 * client 400. Package-private; only the web adapter uses this.
 */
final class WorkflowResponses {

    private WorkflowResponses() {
    }

    /**
     * Builds the HTTP body. Null JSON strings become JSON null.
     *
     * @param snapshot domain snapshot
     * @param mapper Jackson 3 mapper
     * @return HTTP record
     */
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

    /**
     * Maps one step snapshot.
     *
     * @param step domain step
     * @param mapper Jackson 3 mapper
     * @return HTTP step
     */
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

    /**
     * Parses stored JSON text.
     *
     * @param mapper Jackson 3 mapper
     * @param json UTF-8 JSON or null
     * @return tree or null
     * @throws IllegalStateException if stored JSON is corrupt
     */
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
