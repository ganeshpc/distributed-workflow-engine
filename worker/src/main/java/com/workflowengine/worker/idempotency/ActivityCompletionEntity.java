package com.workflowengine.worker.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * One finished activity attempt in the worker database.
 *
 * <p>Written after {@code Activity.execute} returns and before the result is
 * published. A second delivery of the same key loads this row and skips
 * execute. The row is not workflow state: the engine still decides
 * {@code COMPLETED} or {@code FAILED}. {@code output_json} is the activity's
 * JSON text, stored as text so this process does not share the engine schema.
 *
 * <p>Mutable JPA entity. The listener thread inserts it. Two deliveries can
 * race the insert; the primary key keeps one row. No {@code @Version}.
 * Lombok generates the accessors. {@code completedAt} is assigned by
 * PostgreSQL and has no setter.
 */
@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "activity_completion")
@IdClass(ActivityCompletionKey.class)
public class ActivityCompletionEntity {

    @Id
    @Column(name = "workflow_id", nullable = false)
    private UUID workflowId;

    @Id
    @Column(name = "step_name", nullable = false, length = 128)
    private String stepName;

    @Id
    @Column(name = "attempt", nullable = false)
    private int attempt;

    @Column(name = "success", nullable = false)
    private boolean success;

    @Column(name = "output_json")
    private String outputJson;

    @Column(name = "error")
    private String error;

    @Setter(AccessLevel.NONE)
    @Column(name = "completed_at", nullable = false, insertable = false, updatable = false)
    private Instant completedAt;
}
