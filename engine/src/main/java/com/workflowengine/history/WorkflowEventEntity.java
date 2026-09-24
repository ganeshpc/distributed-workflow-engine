package com.workflowengine.history;

import com.workflowengine.api.StepStatus;
import com.workflowengine.api.WorkflowStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Generated;
import org.hibernate.generator.EventType;

import java.time.Instant;
import java.util.UUID;

/**
 * One append-only row in {@code workflow_event}. The projection tables are
 * updated in the same transaction. Rows are never updated.
 *
 * <p>{@code created_at} is assigned by PostgreSQL. Callers must not compare it
 * to {@code Instant.now()}. The type is not thread-safe; it lives in one
 * persistence context.
 */
@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "workflow_event")
@IdClass(WorkflowEventKey.class)
public class WorkflowEventEntity {

    /** Workflow execution. Together with {@code eventId} this is the primary key. */
    @Id
    @Column(name = "workflow_instance_id", nullable = false)
    private UUID workflowInstanceId;

    /** Monotonic id within the execution, starting at 1. */
    @Id
    @Column(name = "event_id", nullable = false)
    private long eventId;

    /** What the transition committed. */
    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 64)
    private HistoryEventType eventType;

    /** Step the transition touched, or null for an instance-only event. */
    @Column(name = "step_name", length = 64)
    private String stepName;

    /** {@code workflow_step.position} when {@code stepName} is set. */
    @Column(name = "step_position")
    private Integer stepPosition;

    /** Attempt after the transition, when the event names a step. */
    @Column(name = "attempt")
    private Integer attempt;

    /** Instance status after this event was applied. */
    @Enumerated(EnumType.STRING)
    @Column(name = "instance_status", nullable = false, length = 32)
    private WorkflowStatus instanceStatus;

    /** Step status after this event, or null when the event does not name a step. */
    @Enumerated(EnumType.STRING)
    @Column(name = "step_status", length = 32)
    private StepStatus stepStatus;

    /** Failure text copied onto the instance, or null. */
    @Column(name = "error")
    private String error;

    /** Extra transition fact, such as the compensation step names. Not a payload log. */
    @Column(name = "detail")
    private String detail;

    /** Database insert time. Not written by the application. */
    @Setter(AccessLevel.NONE)
    @Generated(event = EventType.INSERT)
    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private Instant createdAt;
}
