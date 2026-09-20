package com.workflowengine.domain;

/**
 * One step in a {@link WorkflowDefinition}. Name is the activity lookup key.
 * {@link RetryPolicy} is consulted only when resuming a leftover
 * {@code RUNNING} step; {@code FAILED} is never retried in Phase 2.
 *
 * @param name activity name the invoker looks up, for example {@code CREATE_ORDER}
 * @param retryPolicy attempt cap and backoff for {@code RUNNING} leftovers
 */
public record StepDefinition(String name, RetryPolicy retryPolicy) {

    /**
     * Step with {@link RetryPolicy#defaults()}.
     *
     * @param name activity name
     */
    public StepDefinition(String name) {
        this(name, RetryPolicy.defaults());
    }
}
