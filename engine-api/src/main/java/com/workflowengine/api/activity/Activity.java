package com.workflowengine.api.activity;

public interface Activity {

    String name();

    ActivityResult execute(ActivityContext context);
}
