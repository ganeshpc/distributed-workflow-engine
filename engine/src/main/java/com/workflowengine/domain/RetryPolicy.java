package com.workflowengine.domain;

import java.time.Duration;

/**
 * Attempt cap carried on a step definition.
 *
 * <p>{@code FAILED} stays terminal. Phase 5 republish of a {@code RUNNING}
 * step does not increment {@code attempt} and does not consult
 * {@link #maxAttempts()}. A lost task is bounded by {@code deadline_at}.
 * The cap remains on the definition for a later phase that retries a
 * finished attempt.
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
