package com.workflowengine.worker.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

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
 * race the insert; the primary key keeps one row. No {@code @Version}. The
 * worker does not use Lombok.
 */
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

    @Column(name = "completed_at", nullable = false, insertable = false, updatable = false)
    private Instant completedAt;

    /**
     * @return instance id
     */
    public UUID getWorkflowId() {
        return workflowId;
    }

    /**
     * @param workflowId instance id
     */
    public void setWorkflowId(UUID workflowId) {
        this.workflowId = workflowId;
    }

    /**
     * @return step name
     */
    public String getStepName() {
        return stepName;
    }

    /**
     * @param stepName step name
     */
    public void setStepName(String stepName) {
        this.stepName = stepName;
    }

    /**
     * @return attempt number
     */
    public int getAttempt() {
        return attempt;
    }

    /**
     * @param attempt attempt number
     */
    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    /**
     * @return whether the stored activity result succeeded
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * @param success activity success flag
     */
    public void setSuccess(boolean success) {
        this.success = success;
    }

    /**
     * @return stored output JSON text, or null
     */
    public String getOutputJson() {
        return outputJson;
    }

    /**
     * @param outputJson output JSON text, or null
     */
    public void setOutputJson(String outputJson) {
        this.outputJson = outputJson;
    }

    /**
     * @return stored error text, or null
     */
    public String getError() {
        return error;
    }

    /**
     * @param error error text, or null
     */
    public void setError(String error) {
        this.error = error;
    }

    /**
     * @return database clock when the row was inserted; null before the first flush reload
     */
    public Instant getCompletedAt() {
        return completedAt;
    }
}
