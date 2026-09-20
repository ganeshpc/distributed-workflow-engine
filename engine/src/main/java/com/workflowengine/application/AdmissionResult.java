package com.workflowengine.application;

import com.workflowengine.api.WorkflowSnapshot;

/**
 * Outcome of {@link StartWorkflowService#start}. Distinguishes a new admit
 * snapshot ({@code created = true} → HTTP 201) from an idempotent reload
 * ({@code created = false} → HTTP 200).
 *
 * <p>When {@code created} is true the snapshot is the in-memory admit
 * aggregate ({@code PENDING}, {@code version = 0}). When false it is the
 * current row and may already be {@code RUNNING} or terminal.
 *
 * @param created true if this request won INSERT and submitted the executor
 * @param snapshot admit or existing snapshot; never null
 */
public record AdmissionResult(boolean created, WorkflowSnapshot snapshot) {
}
