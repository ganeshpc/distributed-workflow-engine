package com.workflowengine.api;

/**
 * Lifecycle of one step row as stored on {@code workflow_step.status}.
 *
 * <p>Phase 1 uses the same four values as {@link WorkflowStatus}. Later
 * planned values ({@code TIMED_OUT}, {@code CANCELED}, {@code COMPENSATING})
 * are not added until a phase implements them.
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
    FAILED
}
