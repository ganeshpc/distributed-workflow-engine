package com.workflowengine.web;

import tools.jackson.databind.JsonNode;
import com.workflowengine.api.WorkflowStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * HTTP JSON snapshot. {@code input} and {@code output} are JSON trees, not
 * escaped strings.
 *
 * @param id instance id
 * @param type workflow type
 * @param definitionVersion definition version that ran
 * @param status instance status
 * @param currentStep current or last step; JSON null if never started
 * @param idempotencyKey unique key
 * @param version optimistic-lock version
 * @param input workflow input object
 * @param output last-step output on {@code COMPLETED}; otherwise JSON null
 * @param error failure text; otherwise JSON null
 * @param createdAt database time
 * @param updatedAt database time
 * @param steps ordered by position
 */
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
