package com.workflowengine.history;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Identity of one history event: the workflow execution plus a monotonic
 * {@code eventId} that starts at 1. Two events for the same execution never
 * share an id. The id is assigned inside the transition transaction.
 *
 * <p>Equals and hash code are handwritten over both fields because this is a
 * JPA {@code @IdClass}. Lombok supplies the getters, setters, and constructors.
 * The key is a value object; do not share one mutable instance across threads.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowEventKey implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Workflow execution this event belongs to. */
    private UUID workflowInstanceId;

    /** Monotonic id within that execution. The first event is 1. */
    private long eventId;

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof WorkflowEventKey key)) {
            return false;
        }
        return eventId == key.eventId && Objects.equals(workflowInstanceId, key.workflowInstanceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workflowInstanceId, eventId);
    }
}
