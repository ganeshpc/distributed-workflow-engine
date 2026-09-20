package com.workflowengine.web;

import tools.jackson.databind.JsonNode;

/**
 * HTTP JSON for POST {@code /api/v1/workflows}. Jackson types stay in this
 * package; the engine command uses {@link String} JSON.
 *
 * @param type workflow type
 * @param idempotencyKey required unique key
 * @param input JSON object; null or non-object is 400
 */
public record StartWorkflowRequest(
        String type,
        String idempotencyKey,
        JsonNode input
) {
}
