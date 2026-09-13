package com.workflowengine.activity;

import com.workflowengine.api.activity.Activity;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;
import org.springframework.stereotype.Component;

@Component
public class InProcessActivityInvoker implements ActivityInvoker {

    private final ActivityRegistry registry;

    public InProcessActivityInvoker(ActivityRegistry registry) {
        this.registry = registry;
    }

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
