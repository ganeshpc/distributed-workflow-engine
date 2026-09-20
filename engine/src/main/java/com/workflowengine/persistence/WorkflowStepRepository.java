package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

/**
 * Direct step queries. The executor usually walks steps through the instance
 * aggregate; this repository is for tests and diagnostics.
 */
public interface WorkflowStepRepository extends JpaRepository<WorkflowStepEntity, UUID> {

    /**
     * Returns steps for one instance in definition order.
     *
     * @param workflowInstanceId parent id
     * @return ordered list, possibly empty
     */
    List<WorkflowStepEntity> findByWorkflowInstanceIdOrderByPositionAsc(UUID workflowInstanceId);
}
