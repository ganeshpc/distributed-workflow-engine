package com.workflowengine.support;

import com.workflowengine.worker.WorkerApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;

/**
 * Starts a Testcontainers Kafka broker and the worker process for engine tests.
 *
 * <p>The worker runs in this JVM so stub hooks are visible, and it uses a real
 * broker plus its own Postgres for finished attempts. The {@code test} profile
 * is on so {@code failAt} works. Callers close the worker context, which also
 * stops the worker database. The Kafka container is closed by the test that
 * started it.
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
     * @return worker context; closing it stops the worker database
     */
    public static ConfigurableApplicationContext startWorker(KafkaContainer kafka) {
        PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16")
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(
                                ".*database system is ready to accept connections.*\\s", 2))
                        .withStrategy(Wait.forListeningPort())
                        .withStartupTimeout(Duration.ofSeconds(60)));
        postgres.start();
        ConfigurableApplicationContext context = new SpringApplicationBuilder(WorkerApplication.class)
                .web(WebApplicationType.NONE)
                .run(
                        "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                        "--spring.profiles.active=test",
                        "--spring.kafka.consumer.group-id=workflow-worker-tasks",
                        "--workflow.worker.consumer-group=workflow-worker-tasks",
                        "--spring.flyway.locations=classpath:db/worker",
                        "--spring.datasource.url=" + postgres.getJdbcUrl(),
                        "--spring.datasource.username=" + postgres.getUsername(),
                        "--spring.datasource.password=" + postgres.getPassword()
                );
        context.addApplicationListener((ContextClosedEvent event) -> {
            Thread closer = new Thread(postgres::close, "worker-postgres-close");
            closer.setDaemon(true);
            closer.start();
        });
        return context;
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
