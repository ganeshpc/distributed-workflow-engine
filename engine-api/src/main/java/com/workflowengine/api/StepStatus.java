package com.workflowengine.api;

/**
 * Lifecycle of one step row as stored on {@code workflow_step.status}.
 *
 * <p>The forward walk uses {@code PENDING}, {@code RUNNING}, {@code COMPLETED},
 * and {@code FAILED}. Phase 7 sets a completed forward step to
 * {@code COMPENSATED} when its compensation activity succeeds. Compensation
 * activity rows themselves use the forward statuses. There is no
 * {@code TIMED_OUT} or {@code CANCELED} step status.
 *
 * <p>A crash after a {@code RUNNING} commit leaves the step {@code RUNNING}
 * (a leftover). Phase 1 does not re-invoke it.
 */
public enum StepStatus {

    /** Inserted in the admit transaction. {@code attempt} is 0; timestamps and I/O are null. */
    PENDING,

    /** Step-start transaction committed. Invoke happens after this commit, with no open workflow transaction. */
    RUNNING,

    /** Activity returned success; {@code output_json} and {@code completed_at} are set. */
    COMPLETED,

    /** Activity returned {@code success=false}; {@code error} and {@code completed_at} are set. */
    FAILED,

    /**
     * A completed forward step whose compensation activity has committed
     * success. {@code output_json} stays the forward output.
     */
    COMPENSATED
}
