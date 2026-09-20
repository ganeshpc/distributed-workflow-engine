package com.workflowengine.support;

import com.workflowengine.api.WorkflowStatus;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;

import java.time.Duration;
import java.util.UUID;

/**
 * Polls the repository until the instance is {@code COMPLETED} or {@code FAILED}.
 *
 * <p>Tests must not wait on the request thread. Default callers use a ~5s
 * timeout. Thread-safe only in the sense that each call uses its own loop;
 * the repository must be usable from the test thread.
 */
public final class WorkflowAwait {

    private WorkflowAwait() {
    }

    /**
     * Loads the instance until it is terminal.
     *
     * @param instances repository with steps loaded
     * @param workflowId instance id
     * @param timeout maximum wait
     * @return terminal aggregate
     * @throws AssertionError if the timeout expires
     */
    public static WorkflowInstanceEntity awaitTerminal(
            WorkflowInstanceRepository instances,
            UUID workflowId,
            Duration timeout
    ) {
        long deadline = System.nanoTime() + timeout.toNanos();
        WorkflowInstanceEntity latest = null;
        while (System.nanoTime() < deadline) {
            latest = instances.findById(workflowId).orElseThrow();
            WorkflowStatus status = latest.getStatus();
            if (status == WorkflowStatus.COMPLETED || status == WorkflowStatus.FAILED) {
                return latest;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted waiting for terminal state", ex);
            }
        }
        throw new AssertionError("workflow " + workflowId + " did not reach terminal state; last="
                + (latest == null ? "null" : latest.getStatus() + " v" + latest.getVersion()));
    }
}
