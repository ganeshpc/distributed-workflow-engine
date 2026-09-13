package com.workflowengine.api.activity;

import java.util.UUID;

public record ActivityContext(
        UUID workflowId,
        String workflowType,
        int definitionVersion,
        String stepName,
        int attempt,
        String workflowInputJson,
        String stepInputJson
) {
}
