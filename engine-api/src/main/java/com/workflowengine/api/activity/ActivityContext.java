package com.workflowengine.api.activity;

import java.util.UUID;

/**
 * Input the engine passes into {@link Activity#execute(ActivityContext)}.
 *
 * <p>Phase 1: every step of an instance receives the same
 * {@code workflowInputJson} (JSONB re-serialized value). {@code stepInputJson}
 * is always null; the engine does not chain previous step output.
 *
 * @param workflowId instance id
 * @param workflowType definition type
 * @param definitionVersion version copied at admit
 * @param stepName step being invoked
 * @param attempt current attempt after the step-start transaction incremented it
 * @param workflowInputJson JSON text from {@code workflow_instance.input_json}; never null
 * @param stepInputJson Phase 1 always null
 */
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
