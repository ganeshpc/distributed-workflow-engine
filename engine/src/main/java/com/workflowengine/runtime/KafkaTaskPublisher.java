package com.workflowengine.runtime;

import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.messaging.ActivityMessages;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Persist-then-publish sender for {@code activity.tasks}.
 *
 * <p>There is no outbox row. The step-start transaction commits first; this
 * class publishes afterward. {@link java.util.concurrent.Future#get} waits
 * for the Kafka acknowledgement only. The scanner republishes a stale
 * {@code RUNNING} step when that publish is lost. Not an {@code ActivityInvoker}.
 *
 * <p>Spring singleton. Called from executor and scheduler threads.
 */
@Component
public class KafkaTaskPublisher implements TaskPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final String tasksTopic;

    /**
     * @param kafka string producer configured by Spring Boot
     * @param tasksTopic topic name, default {@code activity.tasks}
     */
    public KafkaTaskPublisher(
            KafkaTemplate<String, String> kafka,
            @Value("${workflow.dispatch.tasks-topic:activity.tasks}") String tasksTopic
    ) {
        this.kafka = kafka;
        this.tasksTopic = tasksTopic;
    }

    /** {@inheritDoc} */
    @Override
    public void publish(ActivityContext task) {
        try {
            kafka.send(tasksTopic, task.workflowId().toString(), ActivityMessages.taskJson(task))
                    .get(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted publishing task", ex);
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException(
                    "task publish failed workflowId=" + task.workflowId() + " step=" + task.stepName(), ex);
        }
    }
}
