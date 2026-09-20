/**
 * Admission, query, and request-thread mapping from persistence into snapshots.
 *
 * <p><strong>Admit-then-run</strong> means admit inserts the instance as
 * {@code PENDING} (all steps {@code PENDING}, {@code version = 0}) and only
 * then submits the executor. HTTP may return that admit snapshot without
 * waiting for the saga.
 *
 * <p>This package talks to repositories and
 * {@link com.workflowengine.runtime.WorkflowExecutor}. Controllers in
 * {@code com.workflowengine.web} must not.
 */
package com.workflowengine.application;
