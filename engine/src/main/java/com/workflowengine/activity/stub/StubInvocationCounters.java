package com.workflowengine.activity.stub;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process-wide invoke counts for durability tests.
 *
 * <p>A blocked stub plus a restarted Spring context must not increment these
 * if the engine did not re-invoke. Thread-safe. Tests must {@link #reset()}
 * between cases because the map is JVM-static.
 */
public final class StubInvocationCounters {

    private static final ConcurrentHashMap<String, AtomicInteger> COUNTERS = new ConcurrentHashMap<>();

    private StubInvocationCounters() {
    }

    /**
     * Increments and returns the new count.
     *
     * @param activityName stub name
     * @return count after increment
     */
    public static int increment(String activityName) {
        return COUNTERS.computeIfAbsent(activityName, key -> new AtomicInteger()).incrementAndGet();
    }

    /**
     * @param activityName stub name
     * @return current count, or 0 if never incremented
     */
    public static int get(String activityName) {
        AtomicInteger counter = COUNTERS.get(activityName);
        return counter == null ? 0 : counter.get();
    }

    /** Clears all counts. Call from {@code @BeforeEach}. */
    public static void reset() {
        COUNTERS.clear();
    }
}
