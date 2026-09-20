package com.workflowengine.activity;

import com.workflowengine.api.activity.Activity;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Phase 1 {@link ActivityInvoker}: look up a local {@link Activity} and call
 * {@link Activity#execute(ActivityContext)} on the current thread.
 *
 * <p>This is a temporary seam. A miss returns
 * {@code UNKNOWN_ACTIVITY:&lt;name&gt;}. A thrown {@link RuntimeException}
 * becomes {@code ACTIVITY_EXCEPTION:…}. Neither case throws to the executor.
 * This type does not interpret {@code failAt}; stubs do.
 *
 * <p>Spring singleton. Invoked from {@code workflow-*} threads. The registry
 * is immutable after startup.
 */
@Component
@RequiredArgsConstructor
public class InProcessActivityInvoker implements ActivityInvoker {

    private final ActivityRegistry registry;

    /**
     * {@inheritDoc}
     *
     * @param context engine-built context; never null
     * @return converted result; never null and never thrown business failure
     */
    @Override
    public ActivityResult invoke(ActivityContext context) {
        Activity activity = registry.findByName(context.stepName()).orElse(null);
        if (activity == null) {
            return new ActivityResult(false, null, "UNKNOWN_ACTIVITY:" + context.stepName());
        }
        try {
            return activity.execute(context);
        } catch (RuntimeException ex) {
            String message = ex.getMessage();
            String error = "ACTIVITY_EXCEPTION:" + ex.getClass().getName();
            if (message != null && !message.isBlank()) {
                error = error + ":" + message;
            }
            return new ActivityResult(false, null, error);
        }
    }
}
