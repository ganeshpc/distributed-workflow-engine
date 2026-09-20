package com.workflowengine.runtime;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single place that may submit {@link WorkflowExecutor}. Admission and the
 * Phase 2 scanner both call {@link #submit(UUID)}.
 *
 * <p>An in-memory inflight set prevents the periodic scanner from starting a
 * second run for a workflow this process is already executing. After a crash
 * the set is empty, so leftovers are submitted again. Idempotent POST must
 * not call this method.
 *
 * <p>Process-wide singleton. {@code submit} is safe from request and scheduler
 * threads. The executor still runs on {@code workflow-*} threads.
 */
@Slf4j
@Component
public class WorkflowDispatcher {

    private final WorkflowExecutor workflowExecutor;
    private final TaskExecutor workflowTaskExecutor;
    private final Set<UUID> inflight = ConcurrentHashMap.newKeySet();

    /**
     * @param workflowExecutor saga runner
     * @param workflowTaskExecutor pool that must not be the request thread
     */
    public WorkflowDispatcher(
            WorkflowExecutor workflowExecutor,
            @Qualifier("workflowTaskExecutor") TaskExecutor workflowTaskExecutor
    ) {
        this.workflowExecutor = workflowExecutor;
        this.workflowTaskExecutor = workflowTaskExecutor;
    }

    /**
     * Submits {@code workflowId} unless this process already has a run in flight.
     *
     * @param workflowId instance to run or resume
     */
    public void submit(UUID workflowId) {
        if (!inflight.add(workflowId)) {
            log.debug("workflow already inflight workflowId={}", workflowId);
            return;
        }
        workflowTaskExecutor.execute(() -> {
            try {
                workflowExecutor.run(workflowId);
            } finally {
                inflight.remove(workflowId);
            }
        });
    }
}
