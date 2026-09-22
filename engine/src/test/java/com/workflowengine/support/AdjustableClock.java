package com.workflowengine.support;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * {@link Clock} whose instant tests can move without sleeping.
 *
 * <p>Phase 3 timeout tests install this as the primary {@code Clock} bean so
 * {@code deadline_at} and the poller share one timeline. Not a production
 * type. Not thread-safe: {@link #advance} and {@link #reset} are for the test
 * thread, and {@code instant} is volatile so a later read on another thread
 * sees the latest value.
 *
 * <p>Failure handling: none. Callers that pass a negative duration move the
 * clock backward, which is allowed so a test can restore an earlier instant.
 */
public final class AdjustableClock extends Clock {

    private final Instant start;
    private final ZoneId zone;
    private volatile Instant instant;

    /**
     * Starts at {@code start} in UTC.
     *
     * @param start initial instant; not null
     */
    public AdjustableClock(Instant start) {
        this.start = start;
        this.zone = ZoneOffset.UTC;
        this.instant = start;
    }

    /**
     * Moves the clock back to the instant passed to the constructor.
     */
    public void reset() {
        instant = start;
    }

    /**
     * Adds {@code duration} to the current instant.
     *
     * @param duration may be zero or negative; not null
     */
    public void advance(Duration duration) {
        instant = instant.plus(duration);
    }

    /** {@inheritDoc} */
    @Override
    public ZoneId getZone() {
        return zone;
    }

    /**
     * Returns a fixed clock at the current instant in {@code zone}. Later
     * advances of this clock do not change that copy.
     *
     * @param zone target zone
     * @return independent clock
     */
    @Override
    public Clock withZone(ZoneId zone) {
        return Clock.fixed(instant, zone);
    }

    /** {@inheritDoc} */
    @Override
    public Instant instant() {
        return instant;
    }
}
