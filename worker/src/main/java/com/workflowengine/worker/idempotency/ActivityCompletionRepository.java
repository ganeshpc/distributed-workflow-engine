package com.workflowengine.worker.idempotency;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Loads and inserts finished attempts.
 *
 * <p>The primary key is the correlation id. Callers use
 * {@link ActivityIdempotencyStore} so a unique-key race reloads the winner
 * instead of publishing a second execution. Spring Data singleton.
 */
public interface ActivityCompletionRepository extends JpaRepository<ActivityCompletionEntity, ActivityCompletionKey> {
}
