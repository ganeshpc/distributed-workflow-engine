package com.workflowengine.worker.idempotency;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Primary key of one finished attempt.
 *
 * <p>This is the Phase 6 correlation id: workflow id, step name, and attempt.
 * The engine already sends those three fields on every task and result. The
 * worker does not invent a second id. Lombok generates the accessors. Equality
 * is the three key fields because JPA uses this type as an {@code @IdClass}.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ActivityCompletionKey implements Serializable {

    private UUID workflowId;
    private String stepName;
    private int attempt;

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
