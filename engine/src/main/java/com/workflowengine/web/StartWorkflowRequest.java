package com.workflowengine.web;

import com.fasterxml.jackson.databind.JsonNode;

public record StartWorkflowRequest(
        String type,
        String idempotencyKey,
        JsonNode input
) {
}
