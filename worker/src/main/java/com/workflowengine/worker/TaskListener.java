package com.workflowengine.worker;

import com.workflowengine.api.activity.ActivityCompletion;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.messaging.ActivityMessages;
import com.workflowengine.api.activity.ActivityResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Consumes one activity task, runs it, and publishes the result.
 *
 * <p>The offset is acknowledged only after this method returns, so a crash
 * during {@code execute} redelivers the same attempt. A malformed payload is
 * logged and skipped so one bad record cannot block the partition. Activity
 * failure is a normal result message.
 *
 * <p>Runs on the Kafka listener thread. One process, concurrency 1 by default.
 */
@Component
public class TaskListener {

    private static final Logger log = LoggerFactory.getLogger(TaskListener.class);

    private final WorkerActivityInvoker invoker;
    private final KafkaTemplate<String, String> kafka;
    private final String resultsTopic;

    /**
     * @param invoker local activity lookup
     * @param kafka result producer; the broker ack is not the engine's result transaction
     * @param resultsTopic {@code activity.results}
     */
    public TaskListener(
            WorkerActivityInvoker invoker,
            KafkaTemplate<String, String> kafka,
            @Value("${workflow.dispatch.results-topic:activity.results}") String resultsTopic
    ) {
        this.invoker = invoker;
        this.kafka = kafka;
        this.resultsTopic = resultsTopic;
    }

    /**
     * Handles one task record.
     *
     * @param payload JSON task; may be malformed
     */
    @KafkaListener(topics = "${workflow.dispatch.tasks-topic:activity.tasks}")
    public void onTask(String payload) {
        ActivityContext task;
        try {
            task = ActivityMessages.task(payload);
        } catch (RuntimeException ex) {
            log.error("discard malformed task", ex);
            return;
        }
        ActivityResult result = invoker.invoke(task);
        ActivityCompletion completion = new ActivityCompletion(
                task.workflowId(),
                task.stepName(),
                task.attempt(),
                result.success(),
                result.outputJson(),
                result.error()
        );
        try {
            kafka.send(resultsTopic, task.workflowId().toString(), ActivityMessages.resultJson(completion))
                    .get(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted publishing result", ex);
        } catch (Exception ex) {
            throw new IllegalStateException("result publish failed workflowId=" + task.workflowId(), ex);
        }
        log.info("result published workflowId={} type={} step={} attempt={} success={}",
                task.workflowId(), task.workflowType(), task.stepName(), task.attempt(), result.success());
    }
}
