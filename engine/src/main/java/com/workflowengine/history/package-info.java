/**
 * Phase 13 append-only history for one workflow execution.
 *
 * <p>Each committed transition writes one or more {@code workflow_event} rows
 * in the same transaction that updates {@code workflow_instance} and
 * {@code workflow_step}. Those rows are the history. The instance and step
 * rows remain the projection that {@code GET} reads. This phase does not
 * replay workflow code. {@link com.workflowengine.runtime.RecoveryScanner}
 * reads the history, then decides from the projection.
 */
package com.workflowengine.history;
