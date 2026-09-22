package com.workflowengine.support;

import com.workflowengine.worker.WorkerApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Starts a Testcontainers Kafka broker and the worker process for engine tests.
 *
 * <p>The worker runs in this JVM so stub hooks are visible, and it uses a real
 * broker. The {@code test} profile is on so {@code failAt} works. Callers close
 * the worker context; the container is closed by the test that started it.
 */
public final class KafkaWorkers {

    private KafkaWorkers() {
    }

    /**
     * Creates a broker container. The caller starts it, or JUnit does when it
     * is a {@code @Container}.
     *
     * @return unstarted container
     */
    public static KafkaContainer newContainer() {
        return new KafkaContainer("apache/kafka-native:3.8.1");
    }

    /**
     * Starts a worker against {@code kafka}.
     *
     * @param kafka running broker
     * @return worker context
     */
    public static ConfigurableApplicationContext startWorker(KafkaContainer kafka) {
        return new SpringApplicationBuilder(WorkerApplication.class)
                .web(WebApplicationType.NONE)
                .run(
                        "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                        "--spring.profiles.active=test"
                );
    }

    /**
     * Engine argument that points Spring Kafka at the test broker.
     *
     * @param kafka running broker
     * @return {@code --spring.kafka.bootstrap-servers=} argument
     */
    public static String bootstrapArg(KafkaContainer kafka) {
        return "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers();
    }
}
