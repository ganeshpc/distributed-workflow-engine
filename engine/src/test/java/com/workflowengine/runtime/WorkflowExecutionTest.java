package com.workflowengine.runtime;

import com.workflowengine.support.KafkaWorkers;
import com.workflowengine.worker.activity.ActivityBlockHook;
import com.workflowengine.worker.activity.CreateOrderActivity;
import com.workflowengine.worker.activity.StubInvocationCounters;
import com.workflowengine.api.StartWorkflowCommand;
import com.workflowengine.api.StepSnapshot;
import com.workflowengine.api.StepStatus;
import com.workflowengine.api.WorkflowSnapshot;
import com.workflowengine.api.WorkflowStatus;
import com.workflowengine.application.AdmissionResult;
import com.workflowengine.application.InvalidStartWorkflowException;
import com.workflowengine.application.StartWorkflowService;
import com.workflowengine.history.HistoryEventType;
import com.workflowengine.history.WorkflowEventEntity;
import com.workflowengine.history.WorkflowHistory;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import com.workflowengine.persistence.WorkflowStepEntity;
import com.workflowengine.support.WorkflowAwait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * State-machine tests without HTTP: happy path {@code version == 10},
 * {@code failAt}, concurrent admit, unknown type, and blocked-stub admit snapshot.
 *
 * <p>Uses Testcontainers Postgres. Does not mock the database.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class WorkflowExecutionTest {

    private static final Duration TERMINAL_TIMEOUT = Duration.ofSeconds(5);
    private static final List<String> ORDER_STEPS = List.of(
            "CREATE_ORDER",
            "RESERVE_INVENTORY",
            "PROCESS_PAYMENT",
            "CREATE_SHIPMENT",
            "SEND_NOTIFICATION"
    );

    @Container
    static KafkaContainer kafka = KafkaWorkers.newContainer();

    private static ConfigurableApplicationContext worker;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeAll
    static void startWorker() {
        worker = KafkaWorkers.startWorker(kafka);
    }

    @AfterAll
    static void stopWorker() {
        if (worker != null) {
            worker.close();
        }
    }

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16")
            .waitingFor(new WaitAllStrategy()
                    .withStrategy(Wait.forLogMessage(
                            ".*database system is ready to accept connections.*\\s", 2))
                    .withStrategy(Wait.forListeningPort())
                    .withStartupTimeout(Duration.ofSeconds(60)));

    @Autowired
    private StartWorkflowService startWorkflowService;

    @Autowired
    private WorkflowInstanceRepository instances;

    @Autowired
    private WorkflowHistory history;

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
    void happyPathCompletesAllStepsAtVersion10() {
        AdmissionResult admission = startWorkflowService.start(command(
                "happy-" + UUID.randomUUID(),
                "{\"customerId\":\"cust-9\",\"amountCents\":4999}"
        ));

        assertAdmitSnapshot(admission);

        WorkflowInstanceEntity done = WorkflowAwait.awaitTerminal(
                instances, admission.snapshot().id(), TERMINAL_TIMEOUT);

        assertThat(done.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(done.getVersion()).isEqualTo(10);
        assertThat(done.getDefinitionVersion()).isEqualTo(1);
        assertThat(done.getCurrentStep()).isEqualTo("SEND_NOTIFICATION");
        assertThat(done.getError()).isNull();
        assertThat(done.getSteps()).hasSize(5);
        assertThat(done.getSteps())
                .extracting(WorkflowStepEntity::getName)
                .containsExactlyElementsOf(ORDER_STEPS);
        assertThat(done.getSteps())
                .extracting(WorkflowStepEntity::getPosition)
                .containsExactly(0, 1, 2, 3, 4);
        assertThat(done.getSteps())
                .extracting(WorkflowStepEntity::getStatus)
                .containsOnly(StepStatus.COMPLETED);
        assertThat(done.getSteps())
                .extracting(WorkflowStepEntity::getAttempt)
                .containsOnly(1);
        assertThat(done.getSteps())
                .allSatisfy(step -> {
                    assertThat(step.getStartedAt()).isNotNull();
                    assertThat(step.getCompletedAt()).isNotNull();
                    assertThat(step.getDeadlineAt()).isNotNull();
                    assertThat(step.getInputJson()).isNull();
                    assertThat(step.getError()).isNull();
                    assertThat(step.getOutputJson()).contains(step.getName());
                });
        WorkflowStepEntity last = done.getSteps().get(4);
        assertThat(done.getOutputJson()).isEqualTo(last.getOutputJson());
        assertThat(done.getOutputJson()).contains("SEND_NOTIFICATION");
        assertThat(history.read(done.getId()))
                .extracting(WorkflowEventEntity::getEventType)
                .containsExactly(
                        HistoryEventType.WORKFLOW_ADMITTED,
                        HistoryEventType.STEP_STARTED, HistoryEventType.STEP_COMPLETED,
                        HistoryEventType.STEP_STARTED, HistoryEventType.STEP_COMPLETED,
                        HistoryEventType.STEP_STARTED, HistoryEventType.STEP_COMPLETED,
                        HistoryEventType.STEP_STARTED, HistoryEventType.STEP_COMPLETED,
                        HistoryEventType.STEP_STARTED, HistoryEventType.STEP_COMPLETED,
                        HistoryEventType.WORKFLOW_COMPLETED);
        assertMonotonicHistory(done.getId(), WorkflowStatus.COMPLETED);
    }

    @Test
    void failAtProcessPaymentCompensatesCompletedSteps() {
        AdmissionResult admission = startWorkflowService.start(command(
                "fail-pay-" + UUID.randomUUID(),
                "{\"customerId\":\"cust-9\",\"failAt\":\"PROCESS_PAYMENT\"}"
        ));
        assertThat(admission.created()).isTrue();

        WorkflowInstanceEntity done = WorkflowAwait.awaitTerminal(
                instances, admission.snapshot().id(), TERMINAL_TIMEOUT);

        assertThat(done.getStatus()).isEqualTo(WorkflowStatus.COMPENSATED);
        assertThat(done.getCurrentStep()).isEqualTo("COMPENSATE_CREATE_ORDER");
        assertThat(done.getVersion()).isEqualTo(10);
        assertThat(done.getOutputJson()).isNull();
        assertThat(done.getError()).isEqualTo("STUB_FORCED_FAILURE");

        List<WorkflowStepEntity> steps = done.getSteps();
        assertThat(steps.get(0).getName()).isEqualTo("CREATE_ORDER");
        assertThat(steps.get(0).getStatus()).isEqualTo(StepStatus.COMPENSATED);
        assertThat(steps.get(1).getName()).isEqualTo("RESERVE_INVENTORY");
        assertThat(steps.get(1).getStatus()).isEqualTo(StepStatus.COMPENSATED);
        assertThat(steps.get(2).getName()).isEqualTo("PROCESS_PAYMENT");
        assertThat(steps.get(2).getStatus()).isEqualTo(StepStatus.FAILED);
        assertThat(steps.get(2).getError()).isEqualTo("STUB_FORCED_FAILURE");
        assertThat(steps.get(3).getStatus()).isEqualTo(StepStatus.PENDING);
        assertThat(steps.get(4).getStatus()).isEqualTo(StepStatus.PENDING);
        assertThat(steps.get(5).getName()).isEqualTo("COMPENSATE_RESERVE_INVENTORY");
        assertThat(steps.get(5).getStatus()).isEqualTo(StepStatus.COMPLETED);
        assertThat(steps.get(5).getAttempt()).isEqualTo(1);
        assertThat(steps.get(6).getName()).isEqualTo("COMPENSATE_CREATE_ORDER");
        assertThat(steps.get(6).getStatus()).isEqualTo(StepStatus.COMPLETED);
        assertThat(steps.get(6).getAttempt()).isEqualTo(1);
        assertThat(history.read(done.getId()))
                .extracting(WorkflowEventEntity::getEventType)
                .containsExactly(
                        HistoryEventType.WORKFLOW_ADMITTED,
                        HistoryEventType.STEP_STARTED, HistoryEventType.STEP_COMPLETED,
                        HistoryEventType.STEP_STARTED, HistoryEventType.STEP_COMPLETED,
                        HistoryEventType.STEP_STARTED, HistoryEventType.STEP_FAILED,
                        HistoryEventType.COMPENSATION_PLANNED, HistoryEventType.COMPENSATION_PLANNED,
                        HistoryEventType.WORKFLOW_COMPENSATING,
                        HistoryEventType.STEP_STARTED, HistoryEventType.STEP_COMPLETED,
                        HistoryEventType.FORWARD_COMPENSATED,
                        HistoryEventType.STEP_STARTED, HistoryEventType.STEP_COMPLETED,
                        HistoryEventType.FORWARD_COMPENSATED, HistoryEventType.WORKFLOW_COMPENSATED);
    }

    @Test
    void failAtCreateOrderLeavesLaterStepsPending() {
        AdmissionResult admission = startWorkflowService.start(command(
                "fail-create-" + UUID.randomUUID(),
                "{\"failAt\":\"CREATE_ORDER\"}"
        ));

        WorkflowInstanceEntity done = WorkflowAwait.awaitTerminal(
                instances, admission.snapshot().id(), TERMINAL_TIMEOUT);

        assertThat(done.getStatus()).isEqualTo(WorkflowStatus.FAILED);
        assertThat(done.getCurrentStep()).isEqualTo("CREATE_ORDER");
        assertThat(done.getVersion()).isEqualTo(2);
        assertThat(done.getError()).isEqualTo("STUB_FORCED_FAILURE");

        List<WorkflowStepEntity> steps = done.getSteps();
        assertThat(steps.get(0).getStatus()).isEqualTo(StepStatus.FAILED);
        assertThat(steps.get(0).getCompletedAt()).isNotNull();
        assertThat(steps.subList(1, 5))
                .allSatisfy(step -> {
                    assertThat(step.getStatus()).isEqualTo(StepStatus.PENDING);
                    assertThat(step.getStartedAt()).isNull();
                    assertThat(step.getAttempt()).isZero();
                });
        assertThat(history.read(done.getId()))
                .extracting(WorkflowEventEntity::getEventType)
                .containsExactly(
                        HistoryEventType.WORKFLOW_ADMITTED,
                        HistoryEventType.STEP_STARTED,
                        HistoryEventType.STEP_FAILED,
                        HistoryEventType.WORKFLOW_FAILED);
    }

    @Test
    void unknownFailAtCompletesSuccessfully() {
        AdmissionResult admission = startWorkflowService.start(command(
                "fail-unknown-" + UUID.randomUUID(),
                "{\"failAt\":\"NOT_A_REAL_STEP\"}"
        ));

        WorkflowInstanceEntity done = WorkflowAwait.awaitTerminal(
                instances, admission.snapshot().id(), TERMINAL_TIMEOUT);

        assertThat(done.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(done.getVersion()).isEqualTo(10);
        assertThat(done.getSteps())
                .extracting(WorkflowStepEntity::getStatus)
                .containsOnly(StepStatus.COMPLETED);
    }

    @Test
    void concurrentAdmitSameKeySubmitsOnce() throws Exception {
        String key = "concurrent-" + UUID.randomUUID();
        StartWorkflowCommand command = command(key, "{\"customerId\":\"cust-9\"}");

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        try {
            Future<AdmissionResult> first = pool.submit(() -> {
                ready.countDown();
                go.await();
                return startWorkflowService.start(command);
            });
            Future<AdmissionResult> second = pool.submit(() -> {
                ready.countDown();
                go.await();
                return startWorkflowService.start(command);
            });
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            go.countDown();

            AdmissionResult a = first.get(5, TimeUnit.SECONDS);
            AdmissionResult b = second.get(5, TimeUnit.SECONDS);

            assertThat(a.created() ^ b.created()).isTrue();
            assertThat(a.snapshot().id()).isEqualTo(b.snapshot().id());
            assertThat(instances.findByIdempotencyKey(key)).hasValueSatisfying(instance ->
                    assertThat(instance.getId()).isEqualTo(a.snapshot().id()));

            WorkflowInstanceEntity done = WorkflowAwait.awaitTerminal(
                    instances, a.snapshot().id(), TERMINAL_TIMEOUT);
            assertThat(done.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void rejectsUnknownTypeAndMissingKey() {
        assertThatThrownBy(() -> startWorkflowService.start(
                new StartWorkflowCommand("UNKNOWN", "key-1", "{}")))
                .isInstanceOf(InvalidStartWorkflowException.class);
        assertThatThrownBy(() -> startWorkflowService.start(
                new StartWorkflowCommand("ORDER", "", "{}")))
                .isInstanceOf(InvalidStartWorkflowException.class);
        assertThatThrownBy(() -> startWorkflowService.start(
                new StartWorkflowCommand("ORDER", "k", "  ")))
                .isInstanceOf(InvalidStartWorkflowException.class);
    }

    private static void assertAdmitSnapshot(AdmissionResult admission) {
        assertThat(admission.created()).isTrue();
        WorkflowSnapshot snapshot = admission.snapshot();
        assertThat(snapshot.status()).isEqualTo(WorkflowStatus.PENDING);
        assertThat(snapshot.version()).isZero();
        assertThat(snapshot.currentStep()).isNull();
        assertThat(snapshot.outputJson()).isNull();
        assertThat(snapshot.definitionVersion()).isEqualTo(1);
        assertThat(snapshot.steps()).hasSize(5);
        assertThat(snapshot.steps())
                .extracting(StepSnapshot::status)
                .containsOnly(StepStatus.PENDING);
        assertThat(snapshot.steps())
                .extracting(StepSnapshot::position)
                .containsExactly(0, 1, 2, 3, 4);
        assertThat(snapshot.steps())
                .extracting(StepSnapshot::attempt)
                .containsOnly(0);
    }

    private void assertMonotonicHistory(UUID workflowId, WorkflowStatus terminal) {
        List<WorkflowEventEntity> events = history.read(workflowId);
        assertThat(events).isNotEmpty();
        for (int i = 0; i < events.size(); i++) {
            assertThat(events.get(i).getEventId()).isEqualTo(i + 1L);
            assertThat(events.get(i).getWorkflowInstanceId()).isEqualTo(workflowId);
        }
        assertThat(events.get(events.size() - 1).getInstanceStatus()).isEqualTo(terminal);
    }

    private static StartWorkflowCommand command(String idempotencyKey, String inputJson) {
        return new StartWorkflowCommand("ORDER", idempotencyKey, inputJson);
    }
}
