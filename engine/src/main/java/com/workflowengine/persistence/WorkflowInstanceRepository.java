package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

import com.workflowengine.api.WorkflowStatus;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Spring Data access for {@link WorkflowInstanceEntity}. Find methods always
 * load {@code steps} so snapshots and the executor see a complete aggregate.
 *
 * <p>Thread safety is the persistence-context boundary. Unique-key races on
 * {@code idempotency_key} surface as {@code DataIntegrityViolationException}.
 */
public interface WorkflowInstanceRepository extends JpaRepository<WorkflowInstanceEntity, UUID> {

    /**
     * Loads by id with steps.
     *
     * @param id workflow id
     * @return empty if missing
     */
    @Override
    @EntityGraph(attributePaths = "steps")
    Optional<WorkflowInstanceEntity> findById(UUID id);

    /**
     * Reloads the winner of an idempotent POST.
     *
     * @param idempotencyKey unique client key
     * @return empty if the key was never admitted
     */
    @EntityGraph(attributePaths = "steps")
    Optional<WorkflowInstanceEntity> findByIdempotencyKey(String idempotencyKey);

    /**
     * Phase 2 leftover scan. Uses {@code idx_workflow_instance_status}.
     * Callers must ignore {@code FAILED} / {@code COMPLETED} even if passed in.
     *
     * @param statuses typically {@code PENDING} and {@code RUNNING}
     * @return aggregates with steps loaded
     */
    @EntityGraph(attributePaths = "steps")
    List<WorkflowInstanceEntity> findByStatusIn(Collection<WorkflowStatus> statuses);
}
