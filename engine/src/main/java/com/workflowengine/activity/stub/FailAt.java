package com.workflowengine.activity.stub;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Reads the demo {@code failAt} field from workflow input JSON.
 *
 * <p>If the field equals {@code activityName}, that stub fails. Parse errors
 * fall back to a substring match so malformed demo JSON still injects
 * failure. Unknown values are not a miss at this layer; the stub simply
 * does not match and succeeds.
 */
final class FailAt {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private FailAt() {
    }

    /**
     * @param workflowInputJson workflow input JSON text; may be null
     * @param activityName stub name
     * @return true when this stub should return {@code STUB_FORCED_FAILURE}
     */
    static boolean matches(String workflowInputJson, String activityName) {
        if (workflowInputJson == null || workflowInputJson.isBlank() || activityName == null) {
            return false;
        }
        try {
            JsonNode node = MAPPER.readTree(workflowInputJson);
            JsonNode failAt = node.get("failAt");
            return failAt != null && !failAt.isNull() && activityName.equals(failAt.asString());
        } catch (Exception ignored) {
            return workflowInputJson.contains("\"failAt\":\"" + activityName + "\"");
        }
    }
}
