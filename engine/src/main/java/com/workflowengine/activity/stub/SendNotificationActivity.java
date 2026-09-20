package com.workflowengine.activity.stub;

import com.workflowengine.api.activity.Activity;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;
import org.springframework.stereotype.Component;

/**
 * In-process stub for {@code SEND_NOTIFICATION}. No mailer; canned JSON only.
 *
 * <p>Last step of the linear ORDER saga. Its output is copied onto the
 * instance when the workflow-complete transaction commits {@code COMPLETED}.
 */
@Component
public class SendNotificationActivity implements Activity {

    /** Step name this stub registers as. */
    public static final String NAME = "SEND_NOTIFICATION";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ActivityResult execute(ActivityContext context) {
        return StubSupport.execute(NAME, context);
    }

    /**
     * @return process-wide invoke count for durability tests
     */
    public static int invocationCount() {
        return StubInvocationCounters.get(NAME);
    }
}
