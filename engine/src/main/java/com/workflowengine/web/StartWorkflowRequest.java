package com.workflowengine.web;

import tools.jackson.databind.JsonNode;

public record StartWorkflowRequest(
        String type,
        String idempotencyKey,
        JsonNode input
) {
}
