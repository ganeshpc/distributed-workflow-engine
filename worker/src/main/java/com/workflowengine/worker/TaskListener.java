package com.workflowengine.worker;

import com.workflowengine.api.activity.ActivityCompletion;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;
import com.workflowengine.worker.idempotency.ActivityIdempotencyStore;
import lombok.extern.slf4j.Slf4j;
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
 * <p>Spring Kafka passes the {@link ActivityContext} in. A value that is not
 * that record is logged and skipped by the consumer error handler. The
 * consumer group is {@code workflow-worker-tasks}, not the engine results
 * group. Activity failure is a normal result message. Runs on the Kafka
 * listener thread. One process, concurrency 1 by default.
 */
@Component
@Slf4j
public class TaskListener {

    private final WorkerActivityInvoker invoker;
    private final ActivityIdempotencyStore completions;
    private final KafkaTemplate<String, ActivityCompletion> kafka;
    private final String resultsTopic;

    /**
     * @param invoker local activity lookup; not called when the attempt is already stored
     * @param completions worker record of finished attempts
     * @param kafka result producer; the value is the {@link ActivityCompletion} record
     * @param resultsTopic {@code activity.results}
     */
    public TaskListener(
            WorkerActivityInvoker invoker,
            ActivityIdempotencyStore completions,
            KafkaTemplate<String, ActivityCompletion> kafka,
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
     * @param task deserialized task; not null when Spring Kafka invokes this method
     */
    @KafkaListener(
            topics = "${workflow.dispatch.tasks-topic:activity.tasks}",
            groupId = "${workflow.worker.consumer-group:workflow-worker-tasks}"
    )
    public void onTask(ActivityContext task) {
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
            kafka.send(resultsTopic, task.workflowId().toString(), completion)
                    .get(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted publishing result", ex);
        } catch (Exception ex) {
            throw new IllegalStateException("result publish failed workflowId=" + task.workflowId(), ex);
        }
    }
}
