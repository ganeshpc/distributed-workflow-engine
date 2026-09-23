/**
 * Worker-owned record of a finished activity attempt.
 *
 * <p>Phase 6 idempotency: the correlation id is
 * {@code (workflowId, stepName, attempt)}, the same identity the engine puts
 * on the task and the result. A row means that attempt already ran to a
 * result. A later delivery publishes the stored result and does not call
 * {@code Activity.execute}. The engine database does not store this table.
 */
package com.workflowengine.worker.idempotency;
