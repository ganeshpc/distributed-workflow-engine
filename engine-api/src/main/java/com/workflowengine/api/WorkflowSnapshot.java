package com.workflowengine.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Read model of one workflow instance plus its steps in {@code position} order.
 *
 * <p>A TX1 snapshot is always {@code PENDING}, {@code version = 0},
 * {@code currentStep} null, {@code outputJson} null. Later GET snapshots
 * reflect committed state, including leftovers. JSON fields are UTF-8 text
 * re-serialized from JSONB, not the original request bytes.
 *
 * <p>Immutable and safe to publish across threads after construction.
 *
 * @param id server-generated instance id
 * @param type definition type that ran
 * @param definitionVersion {@code OrderWorkflowDefinition.version()} copied at admit
 * @param status instance status
 * @param currentStep running, last, or failed step name; null if never started
 * @param idempotencyKey unique client key
 * @param version JPA {@code @Version} after the last committed instance TX
 * @param inputJson workflow input JSON text
 * @param outputJson last step output on {@code COMPLETED}; otherwise null
 * @param error copy of the failed step error; otherwise null
 * @param createdAt database {@code created_at}
 * @param updatedAt database {@code updated_at}
 * @param steps ordered by {@code position}, never null
 */
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
