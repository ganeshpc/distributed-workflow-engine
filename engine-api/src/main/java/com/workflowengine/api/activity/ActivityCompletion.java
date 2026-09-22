package com.workflowengine.api.activity;

import java.util.UUID;

/**
 * Result a worker publishes after {@link Activity#execute(ActivityContext)}.
 *
 * <p>The identity is {@code (workflowId, stepName, attempt)}. Phase 5
 * republishes the same attempt when a task is lost. The engine applies this
 * only when that step is still {@code RUNNING} at that attempt. A later
 * completion, including one that arrives after a timeout, is discarded.
 *
 * <p>Immutable. JSON text on the result topic. No Spring or Jackson types.
 *
 * @param workflowId instance id
 * @param stepName step that ran
 * @param attempt attempt on the task; not incremented by redelivery
 * @param success false when the activity failed or the worker caught an exception
 * @param outputJson activity output JSON text, or null
 * @param error failure text, or null on success
 */
public record ActivityCompletion(
        UUID workflowId,
        String stepName,
        int attempt,
        boolean success,
        String outputJson,
        String error
) {
}
