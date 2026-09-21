package com.workflowengine.domain;

import java.time.Duration;

/**
 * One step in a {@link WorkflowDefinition}. Name is the activity lookup key.
 *
 * <p>{@link RetryPolicy} applies only when resuming a leftover {@code RUNNING}
 * step whose {@link #timeout()} has not elapsed. {@code FAILED} is never
 * retried. {@code timeout} is the per-attempt deadline the Phase 3 poller
 * enforces from {@code workflow_step.deadline_at}. Null means that step has
 * no deadline. The value is not a timer step; timer steps are a later phase.
 *
 * <p>Immutable. Safe to share from a singleton definition.
 *
 * @param name activity name the invoker looks up, for example {@code CREATE_ORDER}
 * @param retryPolicy attempt cap and backoff for {@code RUNNING} leftovers that are still inside the deadline
 * @param timeout per-attempt limit added to the engine clock at step-start and on each running-resume; null means no deadline; zero means due at the moment the attempt starts; negative is rejected
 */
public record StepDefinition(String name, RetryPolicy retryPolicy, Duration timeout) {

    /**
     * Rejects a negative timeout. A null timeout is allowed and means the step never becomes due for the poller.
     *
     * @throws IllegalArgumentException if {@code timeout} is negative or {@code retryPolicy} is null
     */
    public StepDefinition {
        if (retryPolicy == null) {
            throw new IllegalArgumentException("retryPolicy is required");
        }
        if (timeout != null && timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be zero or positive");
        }
    }

    /**
     * Step with {@link RetryPolicy#defaults()} and no timeout.
     *
     * @param name activity name
     */
    public StepDefinition(String name) {
        this(name, RetryPolicy.defaults(), null);
    }

    /**
     * Step with an explicit retry policy and no timeout.
     *
     * @param name activity name
     * @param retryPolicy attempt cap and backoff; not null
     */
    public StepDefinition(String name, RetryPolicy retryPolicy) {
        this(name, retryPolicy, null);
    }
}
