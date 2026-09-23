/**
 * Activity port shared by the engine and the worker. The worker implements
 * {@link com.workflowengine.api.activity.Activity}. The engine publishes
 * {@link com.workflowengine.api.activity.ActivityContext} and applies
 * {@link com.workflowengine.api.activity.ActivityCompletion}. Those records
 * are the Kafka values. This jar does not serialize them; Spring Kafka does.
 */
package com.workflowengine.api.activity;
