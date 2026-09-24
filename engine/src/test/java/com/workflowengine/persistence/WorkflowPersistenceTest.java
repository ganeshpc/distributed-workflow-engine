package com.workflowengine.persistence;

import com.workflowengine.support.KafkaWorkers;
import com.workflowengine.api.StepStatus;
import com.workflowengine.api.WorkflowStatus;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.dao.DataIntegrityViolationException;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Flyway through V3 plus JPA: five steps, unique idempotency key, unique position,
 * {@code @Version} starts at 0, {@code deadline_at} round-trips. No executor and no REST.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class WorkflowPersistenceTest {

    private static final List<String> ORDER_STEPS = List.of(
            "CREATE_ORDER",
            "RESERVE_INVENTORY",
            "PROCESS_PAYMENT",
            "CREATE_SHIPMENT",
            "SEND_NOTIFICATION"
    );

    @Container
    static KafkaContainer kafka = KafkaWorkers.newContainer();

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
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
    private WorkflowInstanceRepository instanceRepository;

    @Autowired
    private WorkflowStepRepository stepRepository;

    @Autowired
    private Flyway flyway;

    @Test
    void persistsInstanceWithFiveStepsAndReloads() {
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("4");

        String idempotencyKey = "order-" + UUID.randomUUID();
        WorkflowInstanceEntity saved = persistOrderInstance(idempotencyKey);

        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getUpdatedAt()).isNotNull();

        WorkflowInstanceEntity reloaded = instanceRepository.findById(saved.getId()).orElseThrow();

        assertThat(reloaded.getIdempotencyKey()).isEqualTo(idempotencyKey);
        assertThat(instanceRepository.findByIdempotencyKey(idempotencyKey))
                .map(WorkflowInstanceEntity::getId)
                .contains(saved.getId());
        assertThat(reloaded.getVersion()).isZero();
        assertThat(reloaded.getStatus()).isEqualTo(WorkflowStatus.PENDING);
        assertThat(reloaded.getCreatedAt()).isNotNull();
        assertThat(reloaded.getUpdatedAt()).isNotNull();
        assertThat(reloaded.getSteps()).hasSize(5);
        assertThat(reloaded.getSteps())
                .extracting(WorkflowStepEntity::getName)
                .containsExactlyElementsOf(ORDER_STEPS);
        assertThat(reloaded.getSteps())
                .extracting(WorkflowStepEntity::getPosition)
                .containsExactly(0, 1, 2, 3, 4);
        assertThat(reloaded.getSteps())
                .extracting(WorkflowStepEntity::getStatus)
                .containsOnly(StepStatus.PENDING);

        List<WorkflowStepEntity> ordered = stepRepository
                .findByWorkflowInstanceIdOrderByPositionAsc(saved.getId());
        assertThat(ordered)
                .extracting(WorkflowStepEntity::getPosition)
                .containsExactly(0, 1, 2, 3, 4);
        assertThat(ordered)
                .extracting(WorkflowStepEntity::getName)
                .containsExactlyElementsOf(ORDER_STEPS);
    }

    @Test
    void flushPopulatesUpdatedAtWithoutReload() {
        WorkflowInstanceEntity saved = persistOrderInstance("ts-" + UUID.randomUUID());
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getUpdatedAt()).isNotNull();

        saved.setStatus(WorkflowStatus.RUNNING);
        WorkflowInstanceEntity afterUpdate = instanceRepository.saveAndFlush(saved);

        assertThat(afterUpdate.getCreatedAt()).isEqualTo(saved.getCreatedAt());
        assertThat(afterUpdate.getUpdatedAt()).isNotNull();
    }

    @Test
    void duplicateIdempotencyKeyIsRejected() {
        String idempotencyKey = "dup-key-" + UUID.randomUUID();
        persistOrderInstance(idempotencyKey);

        assertThatThrownBy(() -> persistOrderInstance(idempotencyKey))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void deadlineAtRoundTripsThroughPostgres() {
        WorkflowInstanceEntity saved = persistOrderInstance("deadline-" + UUID.randomUUID());
        Instant deadline = Instant.parse("2026-01-01T00:05:00Z");
        WorkflowStepEntity step = saved.getSteps().getFirst();
        step.setDeadlineAt(deadline);
        stepRepository.saveAndFlush(step);

        WorkflowStepEntity reloaded = stepRepository.findById(step.getId()).orElseThrow();
        assertThat(reloaded.getDeadlineAt()).isEqualTo(deadline);
    }

    @Test
    void duplicateStepPositionIsRejected() {
        WorkflowInstanceEntity instance = persistOrderInstance("pos-" + UUID.randomUUID());

        WorkflowStepEntity duplicate = newStep("DUPLICATE_POS", 0);
        duplicate.setWorkflowInstance(instanceRepository.getReferenceById(instance.getId()));

        assertThatThrownBy(() -> stepRepository.saveAndFlush(duplicate))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    private WorkflowInstanceEntity persistOrderInstance(String idempotencyKey) {
        WorkflowInstanceEntity instance = new WorkflowInstanceEntity();
        instance.setId(UUID.randomUUID());
        instance.setType("ORDER");
        instance.setDefinitionVersion(1);
        instance.setStatus(WorkflowStatus.PENDING);
        instance.setInputJson("{\"customerId\":\"cust-9\"}");
        instance.setIdempotencyKey(idempotencyKey);

        for (int i = 0; i < ORDER_STEPS.size(); i++) {
            instance.addStep(newStep(ORDER_STEPS.get(i), i));
        }

        return instanceRepository.saveAndFlush(instance);
    }

    private static WorkflowStepEntity newStep(String name, int position) {
        WorkflowStepEntity step = new WorkflowStepEntity();
        step.setId(UUID.randomUUID());
        step.setName(name);
        step.setPosition(position);
        step.setStatus(StepStatus.PENDING);
        step.setAttempt(0);
        return step;
    }
}
