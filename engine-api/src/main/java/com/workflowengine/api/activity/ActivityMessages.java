package com.workflowengine.api.activity;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * JSON codec for Kafka task and result payloads.
 *
 * <p>Shared by the engine and the worker so the wire format lives in
 * {@code engine-api} with no Jackson dependency. One JSON object per message.
 * {@code workflowInputJson} and {@code outputJson} are embedded as JSON strings,
 * not parsed as structure. Malformed text throws {@link IllegalArgumentException}.
 *
 * <p>Stateless and safe to call from any thread.
 */
public final class ActivityMessages {

    private ActivityMessages() {
    }

    /**
     * Encodes a task published to {@code activity.tasks}.
     *
     * @param task context the worker will execute; not null
     * @return UTF-8 JSON text
     */
    public static String taskJson(ActivityContext task) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("kind", "task");
        fields.put("workflowId", task.workflowId().toString());
        fields.put("workflowType", task.workflowType());
        fields.put("definitionVersion", task.definitionVersion());
        fields.put("stepName", task.stepName());
        fields.put("attempt", task.attempt());
        fields.put("workflowInputJson", task.workflowInputJson());
        fields.put("stepInputJson", task.stepInputJson());
        return writeObject(fields);
    }

    /**
     * Decodes a task payload.
     *
     * @param json JSON text; not null
     * @return context
     * @throws IllegalArgumentException if the payload is not a task object
     */
    public static ActivityContext task(String json) {
        Map<String, Object> fields = readObject(json);
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
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("kind", "result");
        fields.put("workflowId", completion.workflowId().toString());
        fields.put("stepName", completion.stepName());
        fields.put("attempt", completion.attempt());
        fields.put("success", completion.success());
        fields.put("outputJson", completion.outputJson());
        fields.put("error", completion.error());
        return writeObject(fields);
    }

    /**
     * Decodes a result payload.
     *
     * @param json JSON text; not null
     * @return completion
     * @throws IllegalArgumentException if the payload is not a result object
     */
    public static ActivityCompletion result(String json) {
        Map<String, Object> fields = readObject(json);
        requireKind(fields, "result");
        Object success = fields.get("success");
        if (!(success instanceof Boolean)) {
            throw new IllegalArgumentException("success must be a boolean");
        }
        return new ActivityCompletion(
                uuid(fields, "workflowId"),
                text(fields, "stepName"),
                integer(fields, "attempt"),
                (Boolean) success,
                nullableText(fields, "outputJson"),
                nullableText(fields, "error")
        );
    }

    private static void requireKind(Map<String, Object> fields, String kind) {
        if (!kind.equals(fields.get("kind"))) {
            throw new IllegalArgumentException("expected kind " + kind);
        }
    }

    private static UUID uuid(Map<String, Object> fields, String name) {
        return UUID.fromString(text(fields, name));
    }

    private static String text(Map<String, Object> fields, String name) {
        String value = nullableText(fields, name);
        if (value == null) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static String nullableText(Map<String, Object> fields, String name) {
        if (!fields.containsKey(name)) {
            throw new IllegalArgumentException("missing " + name);
        }
        Object value = fields.get(name);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String)) {
            throw new IllegalArgumentException(name + " must be a string or null");
        }
        return (String) value;
    }

    private static int integer(Map<String, Object> fields, String name) {
        Object value = fields.get(name);
        if (!(value instanceof Integer)) {
            throw new IllegalArgumentException(name + " must be an integer");
        }
        return (Integer) value;
    }

    private static String writeObject(Map<String, Object> fields) {
        StringBuilder json = new StringBuilder();
        json.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append(quote(entry.getKey())).append(':').append(writeValue(entry.getValue()));
        }
        json.append('}');
        return json.toString();
    }

    private static String writeValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Boolean || value instanceof Integer) {
            return value.toString();
        }
        if (value instanceof String text) {
            return quote(text);
        }
        throw new IllegalArgumentException("unsupported value " + value.getClass().getName());
    }

    private static String quote(String text) {
        StringBuilder out = new StringBuilder(text.length() + 2);
        out.append('"');
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            switch (ch) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (ch < 0x20) {
                        out.append(String.format("\\u%04x", (int) ch));
                    } else {
                        out.append(ch);
                    }
                }
            }
        }
        out.append('"');
        return out.toString();
    }

    private static Map<String, Object> readObject(String json) {
        if (json == null) {
            throw new IllegalArgumentException("json is required");
        }
        Parser parser = new Parser(json);
        parser.skipWs();
        Map<String, Object> fields = parser.object();
        parser.skipWs();
        if (!parser.done()) {
            throw new IllegalArgumentException("trailing data");
        }
        return fields;
    }

    private static final class Parser {
        private final String json;
        private int index;

        private Parser(String json) {
            this.json = json;
        }

        private boolean done() {
            return index >= json.length();
        }

        private void skipWs() {
            while (!done() && Character.isWhitespace(json.charAt(index))) {
                index++;
            }
        }

        private Map<String, Object> object() {
            expect('{');
            Map<String, Object> fields = new LinkedHashMap<>();
            skipWs();
            if (peek('}')) {
                index++;
                return fields;
            }
            while (true) {
                skipWs();
                String key = string();
                skipWs();
                expect(':');
                skipWs();
                fields.put(key, value());
                skipWs();
                if (peek('}')) {
                    index++;
                    return fields;
                }
                expect(',');
            }
        }

        private Object value() {
            if (done()) {
                throw new IllegalArgumentException("truncated value");
            }
            char ch = json.charAt(index);
            if (ch == '"') {
                return string();
            }
            if (ch == 't' && json.startsWith("true", index)) {
                index += 4;
                return Boolean.TRUE;
            }
            if (ch == 'f' && json.startsWith("false", index)) {
                index += 5;
                return Boolean.FALSE;
            }
            if (ch == 'n' && json.startsWith("null", index)) {
                index += 4;
                return null;
            }
            if (ch == '-' || (ch >= '0' && ch <= '9')) {
                return number();
            }
            throw new IllegalArgumentException("unexpected value at " + index);
        }

        private Integer number() {
            int start = index;
            if (peek('-')) {
                index++;
            }
            if (done() || json.charAt(index) < '0' || json.charAt(index) > '9') {
                throw new IllegalArgumentException("expected number");
            }
            while (!done() && json.charAt(index) >= '0' && json.charAt(index) <= '9') {
                index++;
            }
            try {
                return Integer.valueOf(json.substring(start, index));
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("invalid number");
            }
        }

        private String string() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (!done()) {
                char ch = json.charAt(index++);
                if (ch == '"') {
                    return out.toString();
                }
                if (ch != '\\') {
                    out.append(ch);
                    continue;
                }
                if (done()) {
                    throw new IllegalArgumentException("truncated escape");
                }
                char esc = json.charAt(index++);
                switch (esc) {
                    case '"', '\\', '/' -> out.append(esc);
                    case 'b' -> out.append('\b');
                    case 'f' -> out.append('\f');
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    case 't' -> out.append('\t');
                    case 'u' -> out.append(unicode());
                    default -> throw new IllegalArgumentException("bad escape");
                }
            }
            throw new IllegalArgumentException("unterminated string");
        }

        private char unicode() {
            if (index + 4 > json.length()) {
                throw new IllegalArgumentException("truncated unicode escape");
            }
            String hex = json.substring(index, index + 4);
            index += 4;
            try {
                return (char) Integer.parseInt(hex, 16);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("bad unicode escape");
            }
        }

        private boolean peek(char ch) {
            return !done() && json.charAt(index) == ch;
        }

        private void expect(char ch) {
            if (!peek(ch)) {
                throw new IllegalArgumentException("expected '" + ch + "'");
            }
            index++;
        }
    }
}
