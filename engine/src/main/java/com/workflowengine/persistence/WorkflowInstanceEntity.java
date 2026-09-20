package com.workflowengine.persistence;

import com.workflowengine.api.WorkflowStatus;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.ColumnTransformer;
import org.hibernate.annotations.Generated;
import org.hibernate.generator.EventType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * JPA mapping of {@code workflow_instance}, the source of truth for one saga.
 *
 * <p>{@code version} is JPA {@code @Version} only. Do not increment it in SQL.
 * Happy-path terminal value is 10. Identity is the UUID, not {@code equals}.
 *
 * <p>Timestamps come from PostgreSQL. JSON is stored as text with a jsonb
 * cast. Unique {@code idempotency_key} is required (1..128). This entity is
 * not thread-safe; one persistence context at a time, guarded by {@code @Version}
 * if two writers collide.
 *
 * <p>Lombok generates getters, setters, and the JPA no-arg constructor.
 */
@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "workflow_instance")
public class WorkflowInstanceEntity {

    /** Server-generated primary key. */
    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    /** Definition type, currently {@code ORDER}. */
    @Column(name = "type", nullable = false, length = 64)
    private String type;

    /** {@code WorkflowDefinition.version()} copied at admit. */
    @Column(name = "definition_version", nullable = false)
    private int definitionVersion;

    /** Instance lifecycle. */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private WorkflowStatus status;

    /** Workflow input JSONB as text. */
    @Column(name = "input_json", nullable = false, columnDefinition = "jsonb")
    @ColumnTransformer(write = "?::jsonb")
    private String inputJson;

    /** Last-step output on {@code COMPLETED}; otherwise null. */
    @Column(name = "output_json", columnDefinition = "jsonb")
    @ColumnTransformer(write = "?::jsonb")
    private String outputJson;

    /** Running, last, or failed step name; null if never started. */
    @Column(name = "current_step", length = 64)
    private String currentStep;

    /** Required unique client retry handle. */
    @Column(name = "idempotency_key", nullable = false, length = 128)
    private String idempotencyKey;

    /**
     * Optimistic lock. The only incrementer. Do not also write
     * {@code version = version + 1} in SQL.
     */
    @Version
    @Column(name = "version", nullable = false)
    private Integer version;

    /** Copy of the failed step error; otherwise null. */
    @Column(name = "error", columnDefinition = "text")
    private String error;

    /** Insert-time database clock. */
    @Setter(AccessLevel.NONE)
    @Generated(event = EventType.INSERT)
    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private Instant createdAt;

    /** Database clock on insert and update. */
    @Setter(AccessLevel.NONE)
    @Generated(event = {EventType.INSERT, EventType.UPDATE})
    @Column(name = "updated_at", nullable = false, insertable = false, updatable = false)
    private Instant updatedAt;

    /** Steps in {@code position} order; cascade persist on admit. */
    @OneToMany(mappedBy = "workflowInstance", cascade = CascadeType.PERSIST)
    @OrderBy("position ASC")
    private List<WorkflowStepEntity> steps = new ArrayList<>();

    /**
     * Adds a step and sets the back-reference for JPA.
     *
     * @param step new step; not null
     */
    public void addStep(WorkflowStepEntity step) {
        steps.add(step);
        step.setWorkflowInstance(this);
    }
}
