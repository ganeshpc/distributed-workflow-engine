package com.workflowengine.activity;

import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;

public interface ActivityInvoker {

    ActivityResult invoke(ActivityContext context);
}
