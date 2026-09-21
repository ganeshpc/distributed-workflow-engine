/**
 * In-process workflow definitions: type, version, and ordered step names.
 *
 * <p>Definitions live in {@code engine}, not {@code engine-api}. The version
 * that ran is copied onto {@code workflow_instance.definition_version} at
 * admit so a later resume knows which graph to use. Step order is
 * {@code position}, not name. {@link com.workflowengine.domain.RetryPolicy}
 * applies when resuming leftover {@code RUNNING} steps that are still inside
 * their {@link com.workflowengine.domain.StepDefinition#timeout()}. A null
 * timeout means no deadline.
 */
package com.workflowengine.domain;
