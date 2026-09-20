package com.workflowengine.activity.stub;

import com.workflowengine.api.activity.Activity;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;
import org.springframework.stereotype.Component;

/**
 * In-process stub for {@code CREATE_SHIPMENT}. No carrier; canned JSON only.
 *
 * <p>Spring singleton invoked from {@code workflow-*} threads.
 */
@Component
public class CreateShipmentActivity implements Activity {

    /** Step name this stub registers as. */
    public static final String NAME = "CREATE_SHIPMENT";

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
