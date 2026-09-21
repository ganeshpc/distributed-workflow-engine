/**
 * Shared test helpers that poll committed workflow state or move the engine clock.
 *
 * <p>Tests wait on {@code GET} or a repository load until {@code COMPLETED} or
 * {@code FAILED}. They must not block the request thread with {@code ?wait=}.
 */
package com.workflowengine.support;
