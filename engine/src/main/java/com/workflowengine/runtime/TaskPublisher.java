package com.workflowengine.runtime;

import com.workflowengine.api.activity.ActivityContext;

/**
 * Publishes one activity task after the step-start transaction has committed.
 *
 * <p>Phase 5 transport. The publisher waits for the broker acknowledgement,
 * not for the worker. A failure leaves the step {@code RUNNING} so the
 * recovery scanner can publish the same attempt again. Implementations must
 * be safe to call from {@code workflow-*} threads and from the scheduler.
 */
public interface TaskPublisher {

    /**
     * Sends {@code task} to the task topic.
     *
     * @param task context for the current attempt; not null
     * @throws RuntimeException when the broker does not acknowledge the record
     */
    void publish(ActivityContext task);
}
