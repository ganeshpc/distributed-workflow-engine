package com.workflowengine.runtime;

import com.workflowengine.WorkflowEngineApplication;
import com.workflowengine.activity.stub.ActivityBlockHook;
import com.workflowengine.activity.stub.CreateOrderActivity;
import com.workflowengine.activity.stub.StubInvocationCounters;
import com.workflowengine.api.StartWorkflowCommand;
import com.workflowengine.api.StepStatus;
import com.workflowengine.api.WorkflowStatus;
import com.workflowengine.application.AdmissionResult;
import com.workflowengine.application.StartWorkflowService;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import com.workflowengine.persistence.WorkflowStepEntity;
import com.workflowengine.support.WorkflowAwait;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
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
 * Crash leftover: a blocked first stub is visible as {@code RUNNING} on a
 * second JDBC connection. A new Spring context against the same database
 * re-invokes that step (Phase 2) and increments {@code attempt}.
 *
 * <p>Inserting a {@code RUNNING} row by hand is not a substitute.
 */
class WorkflowDurabilityTest {

    private static PostgreSQLContainer postgres;

    @BeforeAll
    static void startPostgres() {
        postgres = new PostgreSQLContainer("postgres:16")
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(
                                ".*database system is ready to accept connections.*\\s", 2))
                        .withStrategy(Wait.forListeningPort())
                        .withStartupTimeout(Duration.ofSeconds(60)));
        postgres.start();
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

        ConfigurableApplicationContext first = startContext();
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

        ActivityBlockHook.clear();
        ActivityBlockHook.install(CreateOrderActivity.NAME);

        ConfigurableApplicationContext second = startContext();
        try {
            assertThat(ActivityBlockHook.awaitBlocked(CreateOrderActivity.NAME, Duration.ofSeconds(5)))
                    .isTrue();
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(invocationsAfterBlock + 1);

            WorkflowInstanceRepository instances = second.getBean(WorkflowInstanceRepository.class);
            WorkflowInstanceEntity reloaded = instances.findById(workflowId).orElseThrow();
            assertThat(reloaded.getStatus()).isEqualTo(WorkflowStatus.RUNNING);
            WorkflowStepEntity firstStep = reloaded.getSteps().stream()
                    .filter(step -> step.getPosition() == 0)
                    .findFirst()
                    .orElseThrow();
            assertThat(firstStep.getStatus()).isEqualTo(StepStatus.RUNNING);
            assertThat(firstStep.getAttempt()).isEqualTo(2);
            assertThat(firstStep.getStartedAt()).isNotNull();

            ActivityBlockHook.release(CreateOrderActivity.NAME);
            WorkflowInstanceEntity done = WorkflowAwait.awaitTerminal(
                    instances, workflowId, Duration.ofSeconds(5));
            assertThat(done.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(done.getSteps().getFirst().getAttempt()).isEqualTo(2);
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

    private static ConfigurableApplicationContext startContext() {
        return new SpringApplicationBuilder(WorkflowEngineApplication.class)
                .web(WebApplicationType.NONE)
                .run(
                        "--spring.datasource.url=" + postgres.getJdbcUrl(),
                        "--spring.datasource.username=" + postgres.getUsername(),
                        "--spring.datasource.password=" + postgres.getPassword(),
                        "--spring.datasource.driver-class-name=org.postgresql.Driver",
                        "--spring.jpa.hibernate.ddl-auto=none",
                        "--spring.flyway.enabled=true"
                );
    }
}
