package com.workflowengine.api;

/**
 * Lifecycle of one workflow instance as stored on {@code workflow_instance.status}.
 *
 * <p>This is current-state, not a Temporal history. Phase 1 values are the four
 * below. Do not add {@code CANCELED} or {@code TIMED_OUT} until a phase
 * implements the transition. Spelling is {@code CANCELED} (one L) when that
 * value appears.
 *
 * <p>The enum is immutable and safe to share across threads. Persistence maps
 * it as {@code STRING}.
 */
public enum WorkflowStatus {

    /** Admit transaction: instance exists, no step has started. {@code current_step} is null. */
    PENDING,

    /** At least one step has been started; the saga is in flight or leftover. */
    RUNNING,

    /** Last step committed {@code COMPLETED} in the same workflow-complete transaction as this status. Terminal. */
    COMPLETED,

    /** A step committed {@code FAILED}. Terminal until a retry policy exists. */
    FAILED
}
