package com.workflowengine.web;

import io.swagger.v3.oas.annotations.media.Schema;
import tools.jackson.databind.JsonNode;

/**
 * HTTP JSON for POST {@code /api/v1/workflows}. Jackson types stay in this
 * package; the engine command uses {@link String} JSON.
 *
 * @param type workflow type
 * @param idempotencyKey required unique key
 * @param input JSON object; null or non-object is 400
 */
@Schema(description = "Admit request. type must be ORDER. idempotencyKey is required (1..128).")
public record StartWorkflowRequest(
        @Schema(description = "Workflow type", example = "ORDER", requiredMode = Schema.RequiredMode.REQUIRED)
        String type,
        @Schema(
                description = "Client retry handle; unique per instance",
                example = "order-1001",
                minLength = 1,
                maxLength = 128,
                requiredMode = Schema.RequiredMode.REQUIRED
        )
        String idempotencyKey,
        @Schema(
                description = "Opaque workflow input object. Optional failAt injects a stub failure.",
                example = "{\"customerId\":\"cust-9\",\"amountCents\":4999}",
                requiredMode = Schema.RequiredMode.REQUIRED
        )
        JsonNode input
) {
}
