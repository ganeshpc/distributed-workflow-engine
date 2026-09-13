package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface WorkflowInstanceRepository extends JpaRepository<WorkflowInstanceEntity, UUID> {

    @Override
    @EntityGraph(attributePaths = "steps")
    Optional<WorkflowInstanceEntity> findById(UUID id);

    @EntityGraph(attributePaths = "steps")
    Optional<WorkflowInstanceEntity> findByIdempotencyKey(String idempotencyKey);
}
