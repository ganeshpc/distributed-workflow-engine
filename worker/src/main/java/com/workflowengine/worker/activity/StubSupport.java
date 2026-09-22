package com.workflowengine.worker.activity;

import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;

/**
 * Shared stub body: count, honor block hooks, then {@code failAt} or canned JSON.
 *
 * <p>Not a business service. Runs on the worker listener thread after the engine
 * has committed {@code RUNNING} and published the task. {@code failAt} is
 * interpreted here only when the test profile enabled it.
 */
final class StubSupport {

    private StubSupport() {
    }

    /**
     * Executes one stub step.
     *
     * @param activityName step name
     * @param context engine context whose {@code workflowInputJson} may contain {@code failAt}
     * @return canned success or {@code STUB_FORCED_FAILURE}
     */
    static ActivityResult execute(String activityName, ActivityContext context) {
        StubInvocationCounters.increment(activityName);
        ActivityBlockHook.honor(activityName);
        if (FailAt.matches(context.workflowInputJson(), activityName)) {
            return new ActivityResult(false, null, "STUB_FORCED_FAILURE");
        }
        return new ActivityResult(true, "{\"stub\":true,\"activity\":\"" + activityName + "\"}", null);
    }
}
