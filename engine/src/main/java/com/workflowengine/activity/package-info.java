/**
 * Sync in-process activity dispatch used by Phase 1.
 *
 * <p>{@link com.workflowengine.activity.ActivityInvoker} is the port the
 * executor calls. Today {@link com.workflowengine.activity.InProcessActivityInvoker}
 * looks up a Spring {@link com.workflowengine.api.activity.Activity} bean and
 * runs it on the executor thread. Phase 5 replaces this with
 * dispatch-and-complete-later; do not block on a Kafka future to fake that.
 *
 * <p>The executor must not import {@code com.workflowengine.activity.stub}.
 */
package com.workflowengine.activity;
