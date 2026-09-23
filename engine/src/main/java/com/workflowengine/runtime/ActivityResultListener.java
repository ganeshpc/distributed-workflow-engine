package com.workflowengine.runtime;

import com.workflowengine.api.activity.ActivityCompletion;
import com.workflowengine.messaging.ActivityMessages;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Applies a worker result to the workflow rows.
 *
 * <p>Runs on the engine's Kafka listener thread, not the HTTP thread and not
 * inside {@link WorkflowExecutor#run}. A malformed payload is discarded.
 * Database failures propagate so the record is retried. The executor decides
 * whether the step is still {@code RUNNING}.
 *
 * <p>One consumer group for this process. A second engine replica is not
 * supported yet.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ActivityResultListener {

    private final WorkflowExecutor executor;

    /**
     * Consumes one result record.
     *
     * @param payload JSON completion; skipped when it does not parse
     */
    @KafkaListener(topics = "${workflow.dispatch.results-topic:activity.results}")
    public void onResult(String payload) {
        ActivityCompletion completion;
        try {
            completion = ActivityMessages.result(payload);
        } catch (RuntimeException ex) {
            log.error("discard malformed result", ex);
            return;
        }
        executor.onActivityResult(completion);
    }
}
