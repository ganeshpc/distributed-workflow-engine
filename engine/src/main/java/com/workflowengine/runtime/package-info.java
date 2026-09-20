/**
 * The linear state machine: commit a step transition, then invoke, never the
 * reverse.
 *
 * <p><strong>Commit-before-invoke</strong> means each unit of work is its own
 * committed transaction ({@code PENDING→RUNNING}, mid-step {@code COMPLETED},
 * last-step instance {@code COMPLETED}, or {@code FAILED}).
 * {@link com.workflowengine.api.activity.Activity#execute} runs with no open
 * workflow transaction.
 *
 * <p>A <strong>leftover</strong> is committed state left after a crash:
 * never-started {@code PENDING}, or a {@code RUNNING} step mid-invoke. Phase 2
 * {@link com.workflowengine.runtime.RecoveryScanner} resumes those.
 * {@code FAILED} stays terminal.
 */
package com.workflowengine.runtime;
