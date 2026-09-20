package com.workflowengine.web;

import tools.jackson.databind.JsonNode;
import com.workflowengine.api.StepStatus;

import java.time.Instant;

public record StepResponse(
        String name,
        int position,
        StepStatus status,
        int attempt,
        JsonNode output,
        String error,
        Instant startedAt,
        Instant completedAt
) {
}
