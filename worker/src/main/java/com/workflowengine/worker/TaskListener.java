package com.workflowengine.worker;

import com.workflowengine.api.activity.ActivityCompletion;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityMessages;
import com.workflowengine.api.activity.ActivityResult;
import com.workflowengine.worker.idempotency.ActivityIdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Consumes one activity task and publishes the result for that attempt.
 *
 * <p>Phase 6: if {@link ActivityIdempotencyStore} already has
 * {@code (workflowId, stepName, attempt)}, this listener publishes that stored
 * result and does not call the activity. Otherwise it executes, commits the
 * completion, then publishes the committed row. The offset is acknowledged
 * only after this method returns, so a crash before the commit redelivers and
 * may execute again. A crash after the commit republishes without executing.
 *
 * <p>The consumer group is {@code workflow-worker-tasks}, not the engine
 * results group. A malformed payload is logged and skipped so one bad record
 * cannot block the partition. Activity failure is a normal result message.
 * Runs on the Kafka listener thread. One process, concurrency 1 by default.
 */
@Component
public class TaskListener {

    private static final Logger log = LoggerFactory.getLogger(TaskListener.class);

    private final WorkerActivityInvoker invoker;
    private final ActivityIdempotencyStore completions;
    private final KafkaTemplate<String, String> kafka;
    private final String resultsTopic;

    /**
     * @param invoker local activity lookup; not called when the attempt is already stored
     * @param completions worker record of finished attempts
     * @param kafka result producer; the broker ack is not the engine's result transaction
     * @param resultsTopic {@code activity.results}
     */
    public TaskListener(
            WorkerActivityInvoker invoker,
            ActivityIdempotencyStore completions,
            KafkaTemplate<String, String> kafka,
            @Value("${workflow.dispatch.results-topic:activity.results}") String resultsTopic
    ) {
        this.invoker = invoker;
        this.completions = completions;
        this.kafka = kafka;
        this.resultsTopic = resultsTopic;
    }

    /**
     * Handles one task record.
     *
     * @param payload JSON task; may be malformed
     */
    @KafkaListener(
            topics = "${workflow.dispatch.tasks-topic:activity.tasks}",
            groupId = "${workflow.worker.consumer-group:workflow-worker-tasks}"
    )
    public void onTask(String payload) {
        ActivityContext task;
        try {
            task = ActivityMessages.task(payload);
        } catch (RuntimeException ex) {
            log.error("discard malformed task", ex);
            return;
        }
        ActivityCompletion stored = completions
                .find(task.workflowId(), task.stepName(), task.attempt())
                .orElse(null);
        if (stored != null) {
            publish(task, stored);
            log.info("result republished workflowId={} type={} step={} attempt={} success={}",
                    task.workflowId(), task.workflowType(), task.stepName(), task.attempt(), stored.success());
            return;
        }
        ActivityResult result = invoker.invoke(task);
        ActivityCompletion completion = completions.record(task, result);
        publish(task, completion);
        log.info("result published workflowId={} type={} step={} attempt={} success={}",
                task.workflowId(), task.workflowType(), task.stepName(), task.attempt(), completion.success());
    }

    private void publish(ActivityContext task, ActivityCompletion completion) {
        try {
            kafka.send(resultsTopic, task.workflowId().toString(), ActivityMessages.resultJson(completion))
                    .get(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted publishing result", ex);
        } catch (Exception ex) {
            throw new IllegalStateException("result publish failed workflowId=" + task.workflowId(), ex);
        }
    }
}
