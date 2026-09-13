package com.workflowengine.api;

public record StartWorkflowCommand(
        String type,
        String idempotencyKey,
        String inputJson
) {
}
