package com.workflowengine.web;

import tools.jackson.databind.JsonNode;
import com.workflowengine.api.StepStatus;

import java.time.Instant;

/**
 * HTTP JSON for one step.
 *
 * @param name step name
 * @param position definition index
 * @param status step status
 * @param attempt start count
 * @param output activity output object, or JSON null
 * @param error failure text, or JSON null
 * @param startedAt database time, or JSON null
 * @param completedAt database time, or JSON null
 */
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
