package com.workflowengine.worker;

import com.workflowengine.api.activity.Activity;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Looks up a local {@link Activity} and calls it on the Kafka listener thread.
 *
 * <p>A miss returns {@code UNKNOWN_ACTIVITY}. A thrown exception becomes
 * {@code ACTIVITY_EXCEPTION}. Neither case is thrown to the listener, so the
 * engine still receives a result and can fail the step. Does not read
 * {@code failAt}; the stubs do, and only in the test profile.
 *
 * <p>Spring singleton. The map is immutable after construction.
 */
@Component
public class WorkerActivityInvoker {

    private final Map<String, Activity> byName;

    /**
     * Indexes every activity bean.
     *
     * @param activities discovered stubs; names must be unique
     * @throws IllegalStateException if two activities share a name
     */
    public WorkerActivityInvoker(List<Activity> activities) {
        Map<String, Activity> map = new HashMap<>();
        for (Activity activity : activities) {
            Activity previous = map.put(activity.name(), activity);
            if (previous != null) {
                throw new IllegalStateException("duplicate activity: " + activity.name());
            }
        }
        this.byName = Map.copyOf(map);
    }

    /**
     * Runs one task.
     *
     * @param context decoded task; not null
     * @return a result the publisher can send; never null
     */
    public ActivityResult invoke(ActivityContext context) {
        Activity activity = byName.get(context.stepName());
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
