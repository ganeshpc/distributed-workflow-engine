/**
 * Activity port: the contract a worker (in-process stub today, out-of-process
 * later) implements to execute one workflow step.
 *
 * <p>{@link com.workflowengine.api.activity.Activity} is the business operation.
 * {@link com.workflowengine.api.activity.ActivityContext} is the input the
 * engine supplies. {@link com.workflowengine.api.activity.ActivityResult} is
 * the success-or-failure payload the executor commits after invoke.
 */
package com.workflowengine.api.activity;
