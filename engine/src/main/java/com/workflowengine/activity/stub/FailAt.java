package com.workflowengine.activity.stub;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

final class FailAt {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private FailAt() {
    }

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
