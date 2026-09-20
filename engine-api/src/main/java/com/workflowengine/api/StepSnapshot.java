package com.workflowengine.api;

import java.time.Instant;

/**
 * Read model of one step row. Order is {@code position}, not name or
 * {@code startedAt}.
 *
 * @param name step name from the definition
 * @param position zero-based index in the definition list
 * @param status step status
 * @param attempt increment on each step-start transaction (0 while {@code PENDING})
 * @param outputJson activity output JSON text, or null
 * @param error failure message, or null
 * @param startedAt database time of the step-start transaction, or null
 * @param completedAt database time of success or failure, or null
 */
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
