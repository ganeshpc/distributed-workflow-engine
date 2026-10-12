package com.workflowengine.history;

/**
 * Kind of one append-only history event. The name is stored on
 * {@code workflow_event.event_type}. Phase 13 records transitions the
 * current-state executor already commits. It does not replay workflow code.
 *
 * <p>The enum is immutable and safe to share across threads.
 */
public enum HistoryEventType {

    /** Admit transaction inserted the instance and its forward steps as {@code PENDING}. */
    WORKFLOW_ADMITTED,

    /** Step-start transaction moved a step to {@code RUNNING} and incremented {@code attempt}. */
    STEP_STARTED,

    /** A step committed {@code COMPLETED}. The instance may still be in flight. */
    STEP_COMPLETED,

    /** The last forward step and the instance committed {@code COMPLETED} together. */
    WORKFLOW_COMPLETED,

    /** A step committed {@code FAILED}. The instance status is a following event. */
    STEP_FAILED,

    /** Compensation rows were inserted for completed forward steps, newest first. */
    COMPENSATION_PLANNED,

    /** The instance moved to {@code COMPENSATING} after a forward failure. */
    WORKFLOW_COMPENSATING,

    /** A forward step moved to {@code COMPENSATED} because its compensation activity succeeded. */
    FORWARD_COMPENSATED,

    /** The last compensation step and the instance committed {@code COMPENSATED} together. */
    WORKFLOW_COMPENSATED,

    /** The instance committed {@code FAILED}. No compensation walk follows. */
    WORKFLOW_FAILED
}
