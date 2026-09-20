package com.workflowengine.domain;

import java.time.Duration;

/**
 * How the Phase 2 scanner treats a leftover {@code RUNNING} step.
 *
 * <p>{@code FAILED} stays terminal regardless of this policy. Crash resume of
 * {@code RUNNING} increments {@code attempt}; when the next attempt would
 * exceed {@link #maxAttempts()}, the executor records poison
 * {@code RETRY_EXHAUSTED} and marks the step and instance {@code FAILED}.
 *
 * <p>Immutable. {@link #defaults()} is three attempts and no backoff so
 * recovery tests see a second invoke immediately.
 *
 * @param maxAttempts inclusive cap on {@code workflow_step.attempt} (must be {@code >= 1})
 * @param initialBackoff delay before a leftover {@code RUNNING} step is due; {@link Duration#ZERO} means now
 */
public record RetryPolicy(int maxAttempts, Duration initialBackoff) {

    /**
     * @throws IllegalArgumentException if {@code maxAttempts < 1} or backoff is null/negative
     */
    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (initialBackoff == null || initialBackoff.isNegative()) {
            throw new IllegalArgumentException("initialBackoff must be zero or positive");
        }
    }

    /**
     * ORDER Phase 2 default: three attempts, no delay.
     *
     * @return policy used when {@link StepDefinition} omits an explicit one
     */
    public static RetryPolicy defaults() {
        return new RetryPolicy(3, Duration.ZERO);
    }
}
