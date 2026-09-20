package com.workflowengine.api;

/**
 * Validated admit request used inside the engine. HTTP JSON is mapped to this
 * record before {@linkplain com.workflowengine.api this module} is involved.
 *
 * <p>Immutable. The idempotency key is required (1..128 characters). JSON is
 * already serialized UTF-8 text; this type does not parse it.
 *
 * @param type workflow type, currently only {@code ORDER}
 * @param idempotencyKey client-owned retry handle; unique per instance
 * @param inputJson workflow input as JSON text; never the raw HTTP bytes after JSONB round-trip
 */
public record StartWorkflowCommand(
        String type,
        String idempotencyKey,
        String inputJson
) {
}
