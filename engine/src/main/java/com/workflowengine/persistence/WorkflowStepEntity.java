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

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "workflow_step")
@DynamicUpdate
public class WorkflowStepEntity {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "workflow_instance_id", nullable = false)
    private WorkflowInstanceEntity workflowInstance;

    @Column(name = "name", nullable = false, length = 64)
    private String name;

    @Column(name = "position", nullable = false)
    private int position;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private StepStatus status;

    @Column(name = "attempt", nullable = false)
    private int attempt;

    @Column(name = "input_json", columnDefinition = "jsonb")
    @ColumnTransformer(write = "?::jsonb")
    private String inputJson;

    @Column(name = "output_json", columnDefinition = "jsonb")
    @ColumnTransformer(write = "?::jsonb")
    private String outputJson;

    @Column(name = "error", columnDefinition = "text")
    private String error;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;
}
