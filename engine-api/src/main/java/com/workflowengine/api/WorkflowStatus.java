package com.workflowengine.api;

/**
 * Lifecycle of one workflow instance as stored on {@code workflow_instance.status}.
 *
 * <p>This enum is the current-state status on {@code workflow_instance}. The
 * product destination is a Temporal-style history, and that history is not
 * this type. Do not add {@code CANCELED}
 * or {@code TIMED_OUT} until a phase implements that transition. Spelling is
 * {@code CANCELED} (one L) when that value appears. Phase 7 adds
 * {@code COMPENSATING} and {@code COMPENSATED} for a linear reverse walk.
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

    /**
     * A forward step committed {@code FAILED} and no earlier step had completed,
     * or a compensation step failed. Terminal. Not retried.
     */
    FAILED,

    /**
     * A forward step failed after one or more earlier steps completed. The
     * executor is walking those steps backward. Not terminal.
     */
    COMPENSATING,

    /**
     * Every compensation step committed success in the same transaction that
     * set this status. Terminal. Instance {@code output_json} stays null.
     * {@code error} remains the forward failure that started the walk.
     */
    COMPENSATED
}
