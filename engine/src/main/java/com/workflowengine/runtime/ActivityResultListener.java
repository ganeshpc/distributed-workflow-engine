package com.workflowengine.runtime;

import com.workflowengine.api.activity.ActivityCompletion;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Applies a worker result to the workflow rows.
 *
 * <p>Runs on the engine's Kafka listener thread, not the HTTP thread and not
 * inside {@link WorkflowExecutor#run}. Spring Kafka deserializes the JSON
 * value into {@link ActivityCompletion} before this method runs. A payload
 * that is not that record is logged and skipped by the consumer error handler.
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
     * @param completion result record Spring Kafka deserialized from the JSON value; not null
     */
    @KafkaListener(topics = "${workflow.dispatch.results-topic:activity.results}")
    public void onResult(ActivityCompletion completion) {
        executor.onActivityResult(completion);
    }
}
