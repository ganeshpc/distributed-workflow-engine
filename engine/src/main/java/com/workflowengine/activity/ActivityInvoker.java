package com.workflowengine.activity;

import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;

/**
 * Sync port the executor uses to run one step.
 *
 * <p>Phase 1 is in-process. Phase 5 rewrites the executor to dispatch and
 * complete later; this interface stays a blocking call until then. Do not
 * implement Kafka by blocking on {@code future.get()}.
 *
 * <p>Implementations must not throw for lookup misses or activity exceptions.
 * Return {@link ActivityResult} with {@code success=false} instead. Called on
 * {@code workflow-*} threads with no open workflow transaction.
 */
public interface ActivityInvoker {

    /**
     * Invokes the activity named by {@code context.stepName()}.
     *
     * @param context engine-built context; never null
     * @return result the executor will commit; never null
     */
    ActivityResult invoke(ActivityContext context);
}
