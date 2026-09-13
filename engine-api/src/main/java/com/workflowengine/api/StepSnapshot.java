package com.workflowengine.api;

import java.time.Instant;

public record StepSnapshot(
        String name,
        int position,
        StepStatus status,
        int attempt,
        String outputJson,
        String error,
        Instant startedAt,
        Instant completedAt
) {
}
