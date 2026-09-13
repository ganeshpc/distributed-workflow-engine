package com.workflowengine.activity.stub;

import com.workflowengine.api.activity.Activity;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;
import org.springframework.stereotype.Component;

@Component
public class SendNotificationActivity implements Activity {

    public static final String NAME = "SEND_NOTIFICATION";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ActivityResult execute(ActivityContext context) {
        return StubSupport.execute(NAME, context);
    }

    public static int invocationCount() {
        return StubInvocationCounters.get(NAME);
    }
}
