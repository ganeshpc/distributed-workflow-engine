package com.workflowengine.history;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

/**
 * Append and read {@code workflow_event}. Reads return events in {@code event_id}
 * order. The next id is one more than the current maximum for that execution.
 *
 * <p>Callers append only inside the transition transaction that updates the
 * projection. The repository is a Spring singleton and holds no per-request state.
 */
public interface WorkflowEventRepository extends JpaRepository<WorkflowEventEntity, WorkflowEventKey> {

    /**
     * History for one execution, oldest event first.
     *
     * @param workflowInstanceId execution id
     * @return empty when this execution has no events
     */
    List<WorkflowEventEntity> findByWorkflowInstanceIdOrderByEventIdAsc(UUID workflowInstanceId);

    /**
     * Highest {@code event_id} already stored for the execution, or 0.
     *
     * @param workflowInstanceId execution id
     * @return 0 when the execution has no events yet
     */
    @Query("select coalesce(max(e.eventId), 0) from WorkflowEventEntity e where e.workflowInstanceId = :workflowInstanceId")
    long maxEventId(@Param("workflowInstanceId") UUID workflowInstanceId);
}
