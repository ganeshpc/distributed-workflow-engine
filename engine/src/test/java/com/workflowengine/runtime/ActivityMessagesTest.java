package com.workflowengine.runtime;

import com.workflowengine.api.activity.ActivityCompletion;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityMessages;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trips the Phase 5 Kafka JSON codec, including embedded JSON and escapes.
 */
class ActivityMessagesTest {

    @Test
    void taskAndResultRoundTrip() {
        UUID workflowId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        ActivityContext task = new ActivityContext(
                workflowId,
                "ORDER",
                1,
                "CREATE_ORDER",
                1,
                "{\"customerId\":\"a\\\"b\",\"note\":\"line\n\"}",
                null
        );

        ActivityContext decoded = ActivityMessages.task(ActivityMessages.taskJson(task));
        assertThat(decoded).isEqualTo(task);

        ActivityCompletion result = new ActivityCompletion(
                workflowId, "CREATE_ORDER", 1, false, null, "STUB_FORCED_FAILURE");
        assertThat(ActivityMessages.result(ActivityMessages.resultJson(result))).isEqualTo(result);
    }

    @Test
    void rejectsATaskPayloadAsAResult() {
        ActivityContext task = new ActivityContext(
                UUID.randomUUID(), "ORDER", 1, "CREATE_ORDER", 1, "{}", null);
        assertThatThrownBy(() -> ActivityMessages.result(ActivityMessages.taskJson(task)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
