package com.workflowengine.messaging;

import com.workflowengine.api.activity.ActivityCompletion;
import com.workflowengine.api.activity.ActivityContext;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.UUID;

/**
 * JSON codec for Kafka task and result payloads.
 *
 * <p>Shared by the engine and the worker. One JSON object per message.
 * {@code workflowInputJson} and {@code outputJson} stay JSON text inside
 * string fields; this codec does not interpret them. Jackson reads and
 * writes the object. Malformed text throws {@link IllegalArgumentException}.
 *
 * <p>Stateless and safe to call from any thread. {@code engine-api} does not
 * depend on this class.
 */
public final class ActivityMessages {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private ActivityMessages() {
    }

    /**
     * Encodes a task published to {@code activity.tasks}.
     *
     * @param task context the worker will execute; not null
     * @return UTF-8 JSON text
     */
    public static String taskJson(ActivityContext task) {
        ObjectNode fields = MAPPER.createObjectNode();
        fields.put("kind", "task");
        fields.put("workflowId", task.workflowId().toString());
        fields.put("workflowType", task.workflowType());
        fields.put("definitionVersion", task.definitionVersion());
        fields.put("stepName", task.stepName());
        fields.put("attempt", task.attempt());
        fields.put("workflowInputJson", task.workflowInputJson());
        putNullable(fields, "stepInputJson", task.stepInputJson());
        return write(fields);
    }

    /**
     * Decodes a task payload.
     *
     * @param json JSON text; not null
     * @return context
     * @throws IllegalArgumentException if the payload is not a task object
     */
    public static ActivityContext task(String json) {
        JsonNode fields = readObject(json);
        requireKind(fields, "task");
        return new ActivityContext(
                uuid(fields, "workflowId"),
                text(fields, "workflowType"),
                integer(fields, "definitionVersion"),
                text(fields, "stepName"),
                integer(fields, "attempt"),
                text(fields, "workflowInputJson"),
                nullableText(fields, "stepInputJson")
        );
    }

    /**
     * Encodes a result published to {@code activity.results}.
     *
     * @param completion worker outcome; not null
     * @return UTF-8 JSON text
     */
    public static String resultJson(ActivityCompletion completion) {
        ObjectNode fields = MAPPER.createObjectNode();
        fields.put("kind", "result");
        fields.put("workflowId", completion.workflowId().toString());
        fields.put("stepName", completion.stepName());
        fields.put("attempt", completion.attempt());
        fields.put("success", completion.success());
        putNullable(fields, "outputJson", completion.outputJson());
        putNullable(fields, "error", completion.error());
        return write(fields);
    }

    /**
     * Decodes a result payload.
     *
     * @param json JSON text; not null
     * @return completion
     * @throws IllegalArgumentException if the payload is not a result object
     */
    public static ActivityCompletion result(String json) {
        JsonNode fields = readObject(json);
        requireKind(fields, "result");
        JsonNode success = required(fields, "success");
        if (!success.isBoolean()) {
            throw new IllegalArgumentException("success must be a boolean");
        }
        return new ActivityCompletion(
                uuid(fields, "workflowId"),
                text(fields, "stepName"),
                integer(fields, "attempt"),
                success.asBoolean(),
                nullableText(fields, "outputJson"),
                nullableText(fields, "error")
        );
    }

    private static void putNullable(ObjectNode fields, String name, String value) {
        if (value == null) {
            fields.putNull(name);
        } else {
            fields.put(name, value);
        }
    }

    private static String write(ObjectNode fields) {
        try {
            return MAPPER.writeValueAsString(fields);
        } catch (JacksonException ex) {
            throw new IllegalArgumentException("could not write activity json", ex);
        }
    }

    private static JsonNode readObject(String json) {
        if (json == null) {
            throw new IllegalArgumentException("json is required");
        }
        try (JsonParser parser = MAPPER.createParser(json)) {
            JsonNode node = MAPPER.readTree(parser);
            if (parser.nextToken() != null) {
                throw new IllegalArgumentException("trailing data");
            }
            if (node == null || !node.isObject()) {
                throw new IllegalArgumentException("expected an object");
            }
            return node;
        } catch (JacksonException ex) {
            throw new IllegalArgumentException("malformed activity json", ex);
        }
    }

    private static void requireKind(JsonNode fields, String kind) {
        if (!kind.equals(text(fields, "kind"))) {
            throw new IllegalArgumentException("expected kind " + kind);
        }
    }

    private static UUID uuid(JsonNode fields, String name) {
        String raw = text(fields, name);
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(name + " must be a uuid", ex);
        }
    }

    private static String text(JsonNode fields, String name) {
        String value = nullableText(fields, name);
        if (value == null) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static String nullableText(JsonNode fields, String name) {
        JsonNode value = required(fields, name);
        if (value.isNull()) {
            return null;
        }
        if (!value.isString()) {
            throw new IllegalArgumentException(name + " must be a string or null");
        }
        return value.asString();
    }

    private static int integer(JsonNode fields, String name) {
        JsonNode value = required(fields, name);
        if (!value.isInt()) {
            throw new IllegalArgumentException(name + " must be an integer");
        }
        return value.asInt();
    }

    private static JsonNode required(JsonNode fields, String name) {
        JsonNode value = fields.get(name);
        if (value == null) {
            throw new IllegalArgumentException("missing " + name);
        }
        return value;
    }
}
