package com.workflowengine.worker;

import com.workflowengine.api.activity.ActivityContext;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import com.workflowengine.worker.activity.CreateOrderActivity;
import com.workflowengine.worker.activity.StubInvocationCounters;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A finished attempt is stored in the worker database. Delivering the same
 * task again publishes that result and does not call the activity.
 *
 * <p>Uses Testcontainers Kafka and Postgres. The second delivery is a new
 * produce of the same task payload, which is what the engine scanner does
 * on republish and what a Kafka redelivery looks like to the listener.
 */
class ActivityIdempotencyTest {

    private PostgreSQLContainer postgres;
    private KafkaContainer kafka;
    private ConfigurableApplicationContext worker;

    @BeforeEach
    void startInfra() {
        StubInvocationCounters.reset();
        kafka = new KafkaContainer("apache/kafka-native:3.8.1");
        kafka.start();
        postgres = new PostgreSQLContainer("postgres:16")
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(
                                ".*database system is ready to accept connections.*\\s", 2))
                        .withStrategy(Wait.forListeningPort())
                        .withStartupTimeout(Duration.ofSeconds(60)));
        postgres.start();
        worker = startWorker();
    }

    @AfterEach
    void stopInfra() {
        if (worker != null) {
            worker.close();
        }
        if (kafka != null) {
            kafka.close();
        }
        if (postgres != null) {
            postgres.close();
        }
        StubInvocationCounters.reset();
    }

    @Test
    void secondDeliveryPublishesTheStoredResultWithoutExecuting() throws Exception {
        ActivityContext task = task();
        try (KafkaConsumer<String, String> results = resultsConsumer();
             KafkaProducer<String, ActivityContext> tasks = taskProducer()) {
            results.subscribe(List.of("activity.results"));
            tasks.send(new ProducerRecord<>("activity.tasks", task.workflowId().toString(), task))
                    .get();
            String first = awaitResult(results);
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);

            tasks.send(new ProducerRecord<>("activity.tasks", task.workflowId().toString(), task))
                    .get();
            String second = awaitResult(results);
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);
            assertThat(second).isEqualTo(first);
        }
    }

    @Test
    void restartedWorkerReplaysTheStoredResult() throws Exception {
        ActivityContext task = task();
        try (KafkaConsumer<String, String> results = resultsConsumer();
             KafkaProducer<String, ActivityContext> tasks = taskProducer()) {
            results.subscribe(List.of("activity.results"));
            tasks.send(new ProducerRecord<>("activity.tasks", task.workflowId().toString(), task))
                    .get();
            String first = awaitResult(results);
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);

            worker.close();
            worker = startWorker();

            tasks.send(new ProducerRecord<>("activity.tasks", task.workflowId().toString(), task))
                    .get();
            String second = awaitResult(results);
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);
            assertThat(second).isEqualTo(first);
        }
    }

    private ConfigurableApplicationContext startWorker() {
        return new SpringApplicationBuilder(WorkerApplication.class)
                .web(WebApplicationType.NONE)
                .run(
                        "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                        "--spring.profiles.active=test",
                        "--spring.datasource.url=" + postgres.getJdbcUrl(),
                        "--spring.datasource.username=" + postgres.getUsername(),
                        "--spring.datasource.password=" + postgres.getPassword()
                );
    }

    private ActivityContext task() {
        return new ActivityContext(
                UUID.randomUUID(),
                "ORDER",
                1,
                CreateOrderActivity.NAME,
                1,
                "{\"customerId\":\"cust-9\"}",
                null
        );
    }

    private KafkaProducer<String, ActivityContext> taskProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(properties);
    }

    private KafkaConsumer<String, String> resultsConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "idempotency-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    private static String awaitResult(KafkaConsumer<String, String> results) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, String> records = results.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records) {
                return record.value();
            }
        }
        throw new AssertionError("no activity result arrived");
    }
}
