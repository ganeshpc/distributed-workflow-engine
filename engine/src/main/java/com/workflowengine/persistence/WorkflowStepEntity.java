package com.workflowengine.persistence;

import com.workflowengine.api.StepStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.ColumnTransformer;
import org.hibernate.annotations.DynamicUpdate;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA mapping of {@code workflow_step}. Order is {@code position}, unique per
 * instance together with {@code name}.
 *
 * <p>Phase 1 keeps {@code input_json} SQL {@code NULL}. {@code started_at} and
 * {@code completed_at} are stamped with PostgreSQL {@code now()}. A leftover
 * {@code RUNNING} row is the crash-mid-invoke contract; Phase 1 does not
 * re-invoke it.
 *
 * <p>Not thread-safe. Lombok generates getters, setters, and the JPA constructor.
 */
@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "workflow_step")
@DynamicUpdate
public class WorkflowStepEntity {

    /** Server-generated primary key. */
    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    /** Owning instance. */
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "workflow_instance_id", nullable = false)
    private WorkflowInstanceEntity workflowInstance;

    /** Activity name, unique per instance. */
    @Column(name = "name", nullable = false, length = 64)
    private String name;

    /** Zero-based definition index; unique per instance. */
    @Column(name = "position", nullable = false)
    private int position;

    /** Step lifecycle. */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private StepStatus status;

    /** Incremented on each start TX; 0 while {@code PENDING}. */
    @Column(name = "attempt", nullable = false)
    private int attempt;

    /** Phase 1 always null; chaining is a later product decision. */
    @Column(name = "input_json", columnDefinition = "jsonb")
    @ColumnTransformer(write = "?::jsonb")
    private String inputJson;

    /** Activity output JSON, or null. */
    @Column(name = "output_json", columnDefinition = "jsonb")
    @ColumnTransformer(write = "?::jsonb")
    private String outputJson;

    /** Failure message; set on {@code FAILED}. */
    @Column(name = "error", columnDefinition = "text")
    private String error;

    /** Database clock at start TX. */
    @Column(name = "started_at")
    private Instant startedAt;

    /** Database clock at success or failure TX. */
    @Column(name = "completed_at")
    private Instant completedAt;
}
