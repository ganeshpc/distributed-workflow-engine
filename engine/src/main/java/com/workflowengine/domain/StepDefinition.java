package com.workflowengine.domain;

/**
 * One step in a {@link WorkflowDefinition}. Phase 1 has only a name; retry
 * policy and timeout fields are planned.
 *
 * @param name activity name the invoker looks up, for example {@code CREATE_ORDER}
 */
public record StepDefinition(String name) {
}
