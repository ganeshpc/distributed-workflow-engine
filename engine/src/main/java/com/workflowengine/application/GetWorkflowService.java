package com.workflowengine.application;

import com.workflowengine.api.WorkflowSnapshot;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Loads a committed snapshot for {@code GET /api/v1/workflows/{id}}.
 *
 * <p>Returns leftovers as-is ({@code PENDING} never-started or {@code RUNNING}
 * mid-step). Executor-thread failures are not converted to HTTP 500 here.
 *
 * <p>Request-thread singleton. Misses throw {@link WorkflowNotFoundException}.
 */
@Service
@RequiredArgsConstructor
public class GetWorkflowService {

    private final WorkflowInstanceRepository instances;

    /**
     * Loads instance plus steps ordered by position.
     *
     * @param id workflow id from the path
     * @return current committed snapshot
     * @throws WorkflowNotFoundException when no row exists
     */
    public WorkflowSnapshot get(UUID id) {
        return instances.findById(id)
                .map(WorkflowSnapshots::from)
                .orElseThrow(() -> new WorkflowNotFoundException(id));
    }
}
