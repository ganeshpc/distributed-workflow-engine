package com.workflowengine.history;

import com.workflowengine.api.StepStatus;
import com.workflowengine.api.WorkflowStatus;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowStepEntity;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Writes and reads the Phase 13 history log.
 *
 * <p>{@link #append} must run inside the transaction that updates the
 * projection. It assigns the next {@code event_id} for that execution and
 * flushes so a later append in the same transaction sees it. {@link #read}
 * returns that log in order. Recovery reads it before it uses the projection.
 * This type does not replay workflow code and does not decide the next step.
 *
 * <p>The component is a Spring singleton. Append is confined to the caller's
 * transaction. A missing transaction fails the append rather than opening a
 * second one.
 */
@Component
public class WorkflowHistory {

    private final WorkflowEventRepository events;

    /**
     * @param events history table
     */
    public WorkflowHistory(WorkflowEventRepository events) {
        this.events = events;
    }

    /**
     * Appends one event for a transition that does not name a step.
     *
     * @param instance projection row already updated in this transaction
     * @param type event kind
     * @param detail extra fact, or null
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void append(WorkflowInstanceEntity instance, HistoryEventType type, String detail) {
        append(instance, type, null, null, detail);
    }

    /**
     * Appends one event for a transition that names a step.
     *
     * @param instance projection row already updated in this transaction
     * @param type event kind
     * @param step step the transition touched; its status is the step status stored
     * @param detail extra fact, or null
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void append(WorkflowInstanceEntity instance, HistoryEventType type, WorkflowStepEntity step, String detail) {
        append(instance, type, step, step == null ? null : step.getStatus(), detail);
    }

    /**
     * Reads the execution's history, oldest event first.
     *
     * @param workflowId execution id
     * @return empty when nothing has been appended
     */
    @Transactional(readOnly = true)
    public List<WorkflowEventEntity> read(UUID workflowId) {
        return events.findByWorkflowInstanceIdOrderByEventIdAsc(workflowId);
    }

    /**
     * Assigns {@code max(event_id)+1} and flushes. Step status may differ from
     * the step row when the event describes a different row than {@code step},
     * which is how a forward step is recorded as {@code COMPENSATED}.
     *
     * @param instance projection, not null
     * @param type event kind
     * @param step named step, or null
     * @param stepStatus status stored on the event; ignored when {@code step} is null
     * @param detail extra fact, or null
     */
    private void append(
            WorkflowInstanceEntity instance,
            HistoryEventType type,
            WorkflowStepEntity step,
            StepStatus stepStatus,
            String detail
    ) {
        long next = events.maxEventId(instance.getId()) + 1;
        WorkflowEventEntity event = new WorkflowEventEntity();
        event.setWorkflowInstanceId(instance.getId());
        event.setEventId(next);
        event.setEventType(type);
        event.setInstanceStatus(instance.getStatus() == null ? WorkflowStatus.PENDING : instance.getStatus());
        event.setDetail(detail);
        event.setError(instance.getError());
        if (step != null) {
            event.setStepName(step.getName());
            event.setStepPosition(step.getPosition());
            event.setAttempt(step.getAttempt());
            event.setStepStatus(stepStatus);
        }
        events.saveAndFlush(event);
    }
}
