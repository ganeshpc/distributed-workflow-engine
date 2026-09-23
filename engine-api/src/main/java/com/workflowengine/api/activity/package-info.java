/**
 * Activity port shared by the engine and the worker. The worker implements
 * {@link com.workflowengine.api.activity.Activity}. The engine publishes
 * {@link com.workflowengine.api.activity.ActivityContext} and applies
 * {@link com.workflowengine.api.activity.ActivityCompletion}. JSON stays a
 * {@code String} here. Jackson lives in {@code engine-json}, not in this jar.
 */
package com.workflowengine.api.activity;
