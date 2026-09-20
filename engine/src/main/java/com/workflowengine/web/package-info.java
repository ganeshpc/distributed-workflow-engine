/**
 * HTTP adapters for admit-then-run. Not named {@code api}; that name is the
 * {@code engine-api} module.
 *
 * <p>Request threads validate, persist the admit transaction, and return. They never run
 * {@link com.workflowengine.runtime.WorkflowExecutor}. Clients poll
 * {@code GET /api/v1/workflows/{id}} for a terminal snapshot. There is no
 * list, cancel, signal, or {@code ?wait=} in Phase 1.
 *
 * <p>Error bodies are generic. SQL, stack traces, and connection strings
 * never leave this package.
 */
package com.workflowengine.web;
