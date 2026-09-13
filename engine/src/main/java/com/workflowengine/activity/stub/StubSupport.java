package com.workflowengine.activity.stub;

import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;

final class StubSupport {

    private StubSupport() {
    }

    static ActivityResult execute(String activityName, ActivityContext context) {
        StubInvocationCounters.increment(activityName);
        ActivityBlockHook.honor(activityName);
        if (FailAt.matches(context.workflowInputJson(), activityName)) {
            return new ActivityResult(false, null, "STUB_FORCED_FAILURE");
        }
        return new ActivityResult(true, "{\"stub\":true,\"activity\":\"" + activityName + "\"}", null);
    }
}
