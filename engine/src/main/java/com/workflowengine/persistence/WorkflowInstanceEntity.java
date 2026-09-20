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

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "workflow_instance")
public class WorkflowInstanceEntity {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "type", nullable = false, length = 64)
    private String type;

    @Column(name = "definition_version", nullable = false)
    private int definitionVersion;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private WorkflowStatus status;

    @Column(name = "input_json", nullable = false, columnDefinition = "jsonb")
    @ColumnTransformer(write = "?::jsonb")
    private String inputJson;

    @Column(name = "output_json", columnDefinition = "jsonb")
    @ColumnTransformer(write = "?::jsonb")
    private String outputJson;

    @Column(name = "current_step", length = 64)
    private String currentStep;

    @Column(name = "idempotency_key", nullable = false, length = 128)
    private String idempotencyKey;

    @Version
    @Column(name = "version", nullable = false)
    private Integer version;

    @Column(name = "error", columnDefinition = "text")
    private String error;

    @Setter(AccessLevel.NONE)
    @Generated(event = EventType.INSERT)
    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private Instant createdAt;

    @Setter(AccessLevel.NONE)
    @Generated(event = {EventType.INSERT, EventType.UPDATE})
    @Column(name = "updated_at", nullable = false, insertable = false, updatable = false)
    private Instant updatedAt;

    @OneToMany(mappedBy = "workflowInstance", cascade = CascadeType.PERSIST)
    @OrderBy("position ASC")
    private List<WorkflowStepEntity> steps = new ArrayList<>();

    public void addStep(WorkflowStepEntity step) {
        steps.add(step);
        step.setWorkflowInstance(this);
    }
}
