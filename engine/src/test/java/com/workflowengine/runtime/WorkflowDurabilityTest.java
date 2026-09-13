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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowDurabilityTest {

    private static PostgreSQLContainer<?> postgres;

    @BeforeAll
    static void startPostgres() {
        postgres = new PostgreSQLContainer<>("postgres:16")
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
    void committedRunningIsVisibleAndDoesNotResumeAfterRestart() throws Exception {
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

        ConfigurableApplicationContext second = startContext();
        try {
            WorkflowInstanceRepository instances = second.getBean(WorkflowInstanceRepository.class);
            WorkflowInstanceEntity reloaded = instances.findById(workflowId).orElseThrow();
            assertThat(reloaded.getStatus()).isEqualTo(WorkflowStatus.RUNNING);
            assertThat(reloaded.getCurrentStep()).isEqualTo(CreateOrderActivity.NAME);
            WorkflowStepEntity firstStep = reloaded.getSteps().stream()
                    .filter(step -> step.getPosition() == 0)
                    .findFirst()
                    .orElseThrow();
            assertThat(firstStep.getStatus()).isEqualTo(StepStatus.RUNNING);
            assertThat(firstStep.getAttempt()).isEqualTo(1);
            assertThat(firstStep.getStartedAt()).isNotNull();

            Thread.sleep(300);
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(invocationsAfterBlock);
            WorkflowInstanceEntity still = instances.findById(workflowId).orElseThrow();
            assertThat(still.getStatus()).isEqualTo(WorkflowStatus.RUNNING);
            assertThat(still.getVersion()).isEqualTo(reloaded.getVersion());
        } finally {
            second.close();
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
