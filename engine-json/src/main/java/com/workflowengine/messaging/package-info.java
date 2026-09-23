/**
 * Jackson codec for the activity task and result strings.
 *
 * <p>The engine and the worker both depend on this module. {@code engine-api}
 * stays a JDK jar and still carries JSON as {@code String}. This package is
 * the edge that turns those strings into {@code ActivityContext} and
 * {@code ActivityCompletion}.
 */
package com.workflowengine.messaging;
