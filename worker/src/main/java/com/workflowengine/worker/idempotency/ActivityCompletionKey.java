package com.workflowengine.worker.idempotency;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Primary key of one finished attempt.
 *
 * <p>This is the Phase 6 correlation id: workflow id, step name, and attempt.
 * The engine already sends those three fields on every task and result. The
 * worker does not invent a second id. Immutable value used only as a JPA id.
 */
public final class ActivityCompletionKey implements Serializable {

    private UUID workflowId;
    private String stepName;
    private int attempt;

    /**
     * Required by JPA. Not for callers.
     */
    public ActivityCompletionKey() {
    }

    /**
     * @param workflowId instance id; not null
     * @param stepName activity name; not null
     * @param attempt attempt from the task; the engine does not increment it on republish
     */
    public ActivityCompletionKey(UUID workflowId, String stepName, int attempt) {
        this.workflowId = workflowId;
        this.stepName = stepName;
        this.attempt = attempt;
    }

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

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ActivityCompletionKey key)) {
            return false;
        }
        return attempt == key.attempt
                && Objects.equals(workflowId, key.workflowId)
                && Objects.equals(stepName, key.stepName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workflowId, stepName, attempt);
    }
}
