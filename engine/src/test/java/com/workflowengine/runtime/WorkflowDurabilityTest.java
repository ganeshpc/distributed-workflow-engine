package com.workflowengine.runtime;

import com.workflowengine.WorkflowEngineApplication;
import com.workflowengine.support.KafkaWorkers;
import com.workflowengine.worker.activity.ActivityBlockHook;
import com.workflowengine.worker.activity.CreateOrderActivity;
import com.workflowengine.worker.activity.StubInvocationCounters;
import com.workflowengine.api.StartWorkflowCommand;
import com.workflowengine.api.StepStatus;
import com.workflowengine.api.WorkflowStatus;
import com.workflowengine.application.AdmissionResult;
import com.workflowengine.application.StartWorkflowService;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import com.workflowengine.persistence.WorkflowStepEntity;
import com.workflowengine.support.WorkflowAwait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Crash leftover: a blocked worker is visible as {@code RUNNING} on a second
 * JDBC connection. A new engine republishes that same attempt. The listener
 * is still inside the first execute, so it stores the completion before it
 * reads the republish. The second delivery does not execute. {@code attempt}
 * stays 1.
 *
 * <p>Inserting a {@code RUNNING} row by hand is not a substitute.
 */
class WorkflowDurabilityTest {

    private static PostgreSQLContainer postgres;
    private static KafkaContainer kafka;
    private static ConfigurableApplicationContext worker;

    @BeforeAll
    static void startInfra() {
        kafka = KafkaWorkers.newContainer();
        kafka.start();
        worker = KafkaWorkers.startWorker(kafka);
        postgres = new PostgreSQLContainer("postgres:16")
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(
                                ".*database system is ready to accept connections.*\\s", 2))
                        .withStrategy(Wait.forListeningPort())
                        .withStartupTimeout(Duration.ofSeconds(60)));
        postgres.start();
    }

    @AfterAll
    static void stopInfra() {
        if (worker != null) {
            worker.close();
        }
        if (kafka != null) {
            kafka.close();
        }
    }

    @BeforeEach
    void resetStubs() {
        ActivityBlockHook.clear();
        StubInvocationCounters.reset();
    }

    @AfterEach
    void releaseStubs() {
        ActivityBlockHook.clear();
    }

    @Test
    void committedRunningIsVisibleAndResumesAfterRestart() throws Exception {
        ActivityBlockHook.install(CreateOrderActivity.NAME);
        UUID workflowId;
        int invocationsAfterBlock;

        ConfigurableApplicationContext first = startContext(false);
        try {
            StartWorkflowService start = first.getBean(StartWorkflowService.class);
            AdmissionResult admission = start.start(new StartWorkflowCommand(
                    "ORDER",
                    "durable-" + UUID.randomUUID(),
                    "{\"customerId\":\"cust-9\"}"
            ));
            assertThat(admission.created()).isTrue();
            workflowId = admission.snapshot().id();

            assertThat(ActivityBlockHook.awaitBlocked(CreateOrderActivity.NAME, Duration.ofSeconds(5)))
                    .isTrue();
            invocationsAfterBlock = CreateOrderActivity.invocationCount();
            assertThat(invocationsAfterBlock).isEqualTo(1);

            assertSecondConnectionSeesRunning(workflowId);
        } finally {
            first.close();
        }

        ConfigurableApplicationContext second = startContext(true);
        try {
            ActivityBlockHook.release(CreateOrderActivity.NAME);
            WorkflowInstanceRepository instances = second.getBean(WorkflowInstanceRepository.class);
            WorkflowInstanceEntity done = WorkflowAwait.awaitTerminal(
                    instances, workflowId, Duration.ofSeconds(10));
            assertThat(done.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(done.getVersion()).isEqualTo(10);
            assertThat(done.getSteps().getFirst().getAttempt()).isEqualTo(1);
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(invocationsAfterBlock);
        } finally {
            second.close();
            ActivityBlockHook.clear();
        }
    }

    private static void assertSecondConnectionSeesRunning(UUID workflowId) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT i.status AS instance_status, s.status AS step_status "
                             + "FROM workflow_instance i "
                             + "JOIN workflow_step s ON s.workflow_instance_id = i.id "
                             + "WHERE i.id = ? AND s.position = 0")) {
            statement.setObject(1, workflowId);
            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("instance_status")).isEqualTo("RUNNING");
                assertThat(rs.getString("step_status")).isEqualTo("RUNNING");
            }
        }
    }

    private static ConfigurableApplicationContext startContext(boolean republish) {
        return new SpringApplicationBuilder(WorkflowEngineApplication.class)
                .web(WebApplicationType.NONE)
                .run(
                        "--spring.datasource.url=" + postgres.getJdbcUrl(),
                        "--spring.datasource.username=" + postgres.getUsername(),
                        "--spring.datasource.password=" + postgres.getPassword(),
                        "--spring.datasource.driver-class-name=org.postgresql.Driver",
                        "--spring.jpa.hibernate.ddl-auto=none",
                        "--spring.flyway.enabled=true",
                        KafkaWorkers.bootstrapArg(kafka),
                        "--workflow.dispatch.republish-after-ms=0",
                        "--workflow.recovery.interval-ms=" + (republish ? "200" : "600000")
                );
    }
}
