package com.workflowengine.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record WorkflowSnapshot(
        UUID id,
        String type,
        int definitionVersion,
        WorkflowStatus status,
        String currentStep,
        String idempotencyKey,
        int version,
        String inputJson,
        String outputJson,
        String error,
        Instant createdAt,
        Instant updatedAt,
        List<StepSnapshot> steps
) {
}
