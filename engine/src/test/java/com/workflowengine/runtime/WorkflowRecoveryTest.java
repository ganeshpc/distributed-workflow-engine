package com.workflowengine.runtime;

import com.workflowengine.WorkflowEngineApplication;
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

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 scanner: never-started {@code PENDING} resumes to {@code COMPLETED};
 * terminal {@code FAILED} is left alone across restart.
 */
class WorkflowRecoveryTest {

    private static final List<String> ORDER_STEPS = List.of(
            "CREATE_ORDER",
            "RESERVE_INVENTORY",
            "PROCESS_PAYMENT",
            "CREATE_SHIPMENT",
            "SEND_NOTIFICATION"
    );

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
        StubInvocationCounters.reset();
    }

    @AfterEach
    void resetCounters() {
        StubInvocationCounters.reset();
    }

    @Test
    void neverStartedPendingIsResumedOnStartup() {
        UUID workflowId;
        ConfigurableApplicationContext setup = startContext(false);
        try {
            WorkflowInstanceRepository instances = setup.getBean(WorkflowInstanceRepository.class);
            workflowId = persistPendingOrder(instances, "pending-" + UUID.randomUUID()).getId();
        } finally {
            setup.close();
        }

        // setup used recovery.enabled=false so this row stays PENDING until recovered.

        ConfigurableApplicationContext recovered = startContext(true);
        try {
            WorkflowInstanceRepository instances = recovered.getBean(WorkflowInstanceRepository.class);
            WorkflowInstanceEntity done = WorkflowAwait.awaitTerminal(
                    instances, workflowId, Duration.ofSeconds(5));
            assertThat(done.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(done.getVersion()).isEqualTo(10);
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);
        } finally {
            recovered.close();
        }
    }

    @Test
    void failedWorkflowIsNotRetriedAfterRestart() {
        UUID workflowId;
        int invocations;

        ConfigurableApplicationContext first = startContext(true);
        try {
            StartWorkflowService start = first.getBean(StartWorkflowService.class);
            WorkflowInstanceRepository instances = first.getBean(WorkflowInstanceRepository.class);
            AdmissionResult admission = start.start(new StartWorkflowCommand(
                    "ORDER",
                    "failed-" + UUID.randomUUID(),
                    "{\"failAt\":\"CREATE_ORDER\"}"
            ));
            workflowId = admission.snapshot().id();
            WorkflowInstanceEntity failed = WorkflowAwait.awaitTerminal(
                    instances, workflowId, Duration.ofSeconds(5));
            assertThat(failed.getStatus()).isEqualTo(WorkflowStatus.FAILED);
            invocations = CreateOrderActivity.invocationCount();
            assertThat(invocations).isEqualTo(1);
        } finally {
            first.close();
        }

        ConfigurableApplicationContext second = startContext(true);
        try {
            WorkflowInstanceRepository instances = second.getBean(WorkflowInstanceRepository.class);
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(ex);
            }
            WorkflowInstanceEntity still = instances.findById(workflowId).orElseThrow();
            assertThat(still.getStatus()).isEqualTo(WorkflowStatus.FAILED);
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(invocations);
        } finally {
            second.close();
        }
    }

    private static WorkflowInstanceEntity persistPendingOrder(
            WorkflowInstanceRepository instances,
            String idempotencyKey
    ) {
        WorkflowInstanceEntity instance = new WorkflowInstanceEntity();
        instance.setId(UUID.randomUUID());
        instance.setType("ORDER");
        instance.setDefinitionVersion(1);
        instance.setStatus(WorkflowStatus.PENDING);
        instance.setInputJson("{\"customerId\":\"cust-9\"}");
        instance.setIdempotencyKey(idempotencyKey);
        for (int i = 0; i < ORDER_STEPS.size(); i++) {
            WorkflowStepEntity step = new WorkflowStepEntity();
            step.setId(UUID.randomUUID());
            step.setName(ORDER_STEPS.get(i));
            step.setPosition(i);
            step.setStatus(StepStatus.PENDING);
            step.setAttempt(0);
            instance.addStep(step);
        }
        return instances.saveAndFlush(instance);
    }

    private static ConfigurableApplicationContext startContext(boolean recoveryEnabled) {
        return new SpringApplicationBuilder(WorkflowEngineApplication.class)
                .web(WebApplicationType.NONE)
                .run(
                        "--spring.datasource.url=" + postgres.getJdbcUrl(),
                        "--spring.datasource.username=" + postgres.getUsername(),
                        "--spring.datasource.password=" + postgres.getPassword(),
                        "--spring.datasource.driver-class-name=org.postgresql.Driver",
                        "--spring.jpa.hibernate.ddl-auto=none",
                        "--spring.flyway.enabled=true",
                        "--workflow.recovery.enabled=" + recoveryEnabled,
                        "--workflow.recovery.interval-ms=200"
                );
    }
}
