package com.workflowengine.activity.stub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

final class FailAt {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FailAt() {
    }

    static boolean matches(String workflowInputJson, String activityName) {
        if (workflowInputJson == null || workflowInputJson.isBlank() || activityName == null) {
            return false;
        }
        try {
            JsonNode node = MAPPER.readTree(workflowInputJson);
            JsonNode failAt = node.get("failAt");
            return failAt != null && !failAt.isNull() && activityName.equals(failAt.asText());
        } catch (Exception ignored) {
            return workflowInputJson.contains("\"failAt\":\"" + activityName + "\"");
        }
    }
}
