package com.workflowengine.web;

import tools.jackson.databind.JsonNode;
import com.workflowengine.api.WorkflowStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record WorkflowResponse(
        UUID id,
        String type,
        int definitionVersion,
        WorkflowStatus status,
        String currentStep,
        String idempotencyKey,
        int version,
        JsonNode input,
        JsonNode output,
        String error,
        Instant createdAt,
        Instant updatedAt,
        List<StepResponse> steps
) {
}
