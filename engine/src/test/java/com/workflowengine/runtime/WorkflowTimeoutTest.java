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
import com.workflowengine.definition.OrderWorkflowDefinition;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import com.workflowengine.persistence.WorkflowStepEntity;
import com.workflowengine.support.AdjustableClock;
import com.workflowengine.support.WorkflowAwait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 timeout poller: a due {@code deadline_at} fails the {@code RUNNING}
 * step with {@code TIMED_OUT}, a late success does not overwrite that, and a
 * restart past the deadline does not invoke the stub again.
 *
 * <p>Uses a controllable {@link Clock}. Does not sleep for
 * {@link OrderWorkflowDefinition#STEP_TIMEOUT}.
 */
class WorkflowTimeoutTest {

    private static final Duration STABILITY = Duration.ofSeconds(2);
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final AdjustableClock CLOCK = new AdjustableClock(START);

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
    void resetStubsAndClock() {
        ActivityBlockHook.clear();
        StubInvocationCounters.reset();
        CLOCK.reset();
    }

    @AfterEach
    void releaseStubs() {
        ActivityBlockHook.clear();
        CLOCK.reset();
    }

    @Test
    void overdueRunningStepFailsAndLateSuccessIsDiscarded() throws Exception {
        ActivityBlockHook.install(CreateOrderActivity.NAME);
        ConfigurableApplicationContext context = startContext(CLOCK);
        try {
            StartWorkflowService start = context.getBean(StartWorkflowService.class);
            WorkflowInstanceRepository instances = context.getBean(WorkflowInstanceRepository.class);
            TimeoutPoller poller = context.getBean(TimeoutPoller.class);
            Instant startedAtClock = CLOCK.instant();

            AdmissionResult admission = start.start(new StartWorkflowCommand(
                    "ORDER",
                    "timeout-live-" + UUID.randomUUID(),
                    "{\"customerId\":\"cust-9\"}"
            ));
            UUID workflowId = admission.snapshot().id();
            assertThat(ActivityBlockHook.awaitBlocked(CreateOrderActivity.NAME, Duration.ofSeconds(5))).isTrue();

            WorkflowInstanceEntity running = instances.findById(workflowId).orElseThrow();
            WorkflowStepEntity first = stepAt(running, 0);
            assertThat(running.getStatus()).isEqualTo(WorkflowStatus.RUNNING);
            assertThat(running.getVersion()).isEqualTo(1);
            assertThat(first.getStatus()).isEqualTo(StepStatus.RUNNING);
            assertThat(first.getAttempt()).isEqualTo(1);
            assertThat(first.getDeadlineAt()).isEqualTo(startedAtClock.plus(OrderWorkflowDefinition.STEP_TIMEOUT));

            poller.scan();
            assertThat(instances.findById(workflowId).orElseThrow().getStatus()).isEqualTo(WorkflowStatus.RUNNING);

            CLOCK.advance(OrderWorkflowDefinition.STEP_TIMEOUT.plusMinutes(1));
            poller.scan();

            WorkflowInstanceEntity failed = instances.findById(workflowId).orElseThrow();
            assertTimedOut(failed);
            assertThat(failed.getVersion()).isEqualTo(2);
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);

            ActivityBlockHook.release(CreateOrderActivity.NAME);
            long deadline = System.nanoTime() + STABILITY.toNanos();
            while (System.nanoTime() < deadline) {
                WorkflowInstanceEntity latest = instances.findById(workflowId).orElseThrow();
                assertTimedOut(latest);
                assertThat(latest.getVersion()).isEqualTo(2);
                assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);
                Thread.sleep(50);
            }
        } finally {
            context.close();
        }
    }

    @Test
    void expiredLeftoverIsNotReinvokedAfterRestart() throws Exception {
        ActivityBlockHook.install(CreateOrderActivity.NAME);
        UUID workflowId;

        ConfigurableApplicationContext first = startContext(null);
        try {
            StartWorkflowService start = first.getBean(StartWorkflowService.class);
            WorkflowInstanceRepository instances = first.getBean(WorkflowInstanceRepository.class);
            AdmissionResult admission = start.start(new StartWorkflowCommand(
                    "ORDER",
                    "timeout-restart-" + UUID.randomUUID(),
                    "{\"customerId\":\"cust-9\"}"
            ));
            workflowId = admission.snapshot().id();
            assertThat(ActivityBlockHook.awaitBlocked(CreateOrderActivity.NAME, Duration.ofSeconds(5))).isTrue();

            WorkflowInstanceEntity running = instances.findById(workflowId).orElseThrow();
            assertThat(running.getStatus()).isEqualTo(WorkflowStatus.RUNNING);
            assertThat(stepAt(running, 0).getAttempt()).isEqualTo(1);
            assertThat(stepAt(running, 0).getDeadlineAt()).isNotNull();
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);
        } finally {
            first.close();
        }

        ConfigurableApplicationContext second = startContext(
                Clock.fixed(Instant.parse("2099-01-01T00:00:00Z"), ZoneOffset.UTC));
        try {
            WorkflowInstanceRepository instances = second.getBean(WorkflowInstanceRepository.class);
            WorkflowInstanceEntity failed = WorkflowAwait.awaitTerminal(
                    instances, workflowId, Duration.ofSeconds(5));
            assertTimedOut(failed);
            assertThat(failed.getVersion()).isEqualTo(2);
            assertThat(stepAt(failed, 0).getAttempt()).isEqualTo(1);
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);
        } finally {
            second.close();
        }
    }

    private static void assertTimedOut(WorkflowInstanceEntity instance) {
        assertThat(instance.getStatus()).isEqualTo(WorkflowStatus.FAILED);
        assertThat(instance.getCurrentStep()).isEqualTo("CREATE_ORDER");
        assertThat(instance.getError()).isEqualTo(WorkflowExecutor.TIMED_OUT_ERROR);
        assertThat(instance.getOutputJson()).isNull();
        WorkflowStepEntity first = stepAt(instance, 0);
        assertThat(first.getStatus()).isEqualTo(StepStatus.FAILED);
        assertThat(first.getError()).isEqualTo(WorkflowExecutor.TIMED_OUT_ERROR);
        assertThat(first.getAttempt()).isEqualTo(1);
        assertThat(first.getCompletedAt()).isNotNull();
        for (int position = 1; position < instance.getSteps().size(); position++) {
            WorkflowStepEntity later = stepAt(instance, position);
            assertThat(later.getStatus()).isEqualTo(StepStatus.PENDING);
            assertThat(later.getStartedAt()).isNull();
            assertThat(later.getDeadlineAt()).isNull();
        }
    }

    private static WorkflowStepEntity stepAt(WorkflowInstanceEntity instance, int position) {
        return instance.getSteps().stream()
                .filter(step -> step.getPosition() == position)
                .findFirst()
                .orElseThrow();
    }

    /**
     * Starts an engine against the shared Postgres. {@code clock} is registered
     * as the {@code Clock} bean when non-null, before configuration runs, so
     * deadlines and the poller share it. Null leaves the UTC system clock.
     *
     * @param clock primary clock override, or null
     * @return running context; the caller closes it
     */
    private static ConfigurableApplicationContext startContext(Clock clock) {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(WorkflowEngineApplication.class)
                .web(WebApplicationType.NONE);
        if (clock != null) {
            builder.initializers(applicationContext -> {
                GenericApplicationContext generic = (GenericApplicationContext) applicationContext;
                generic.registerBean("clock", Clock.class, () -> clock);
            });
        }
        return builder.run(
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--spring.datasource.driver-class-name=org.postgresql.Driver",
                "--spring.jpa.hibernate.ddl-auto=none",
                "--spring.flyway.enabled=true",
                "--workflow.recovery.interval-ms=600000",
                "--workflow.timeout.interval-ms=600000",
                KafkaWorkers.bootstrapArg(kafka)
        );
    }
}
