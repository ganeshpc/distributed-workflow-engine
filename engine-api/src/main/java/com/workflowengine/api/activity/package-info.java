/**
 * Activity port shared by the engine and the worker. The worker implements
 * {@link com.workflowengine.api.activity.Activity}. The engine publishes
 * {@link com.workflowengine.api.activity.ActivityContext} and applies
 * {@link com.workflowengine.api.activity.ActivityCompletion}. JSON codecs in
 * this package use the JDK only.
 */
package com.workflowengine.api.activity;
