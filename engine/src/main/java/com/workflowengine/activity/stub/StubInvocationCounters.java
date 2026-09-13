package com.workflowengine.activity.stub;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class StubInvocationCounters {

    private static final ConcurrentHashMap<String, AtomicInteger> COUNTERS = new ConcurrentHashMap<>();

    private StubInvocationCounters() {
    }

    public static int increment(String activityName) {
        return COUNTERS.computeIfAbsent(activityName, key -> new AtomicInteger()).incrementAndGet();
    }

    public static int get(String activityName) {
        AtomicInteger counter = COUNTERS.get(activityName);
        return counter == null ? 0 : counter.get();
    }

    public static void reset() {
        COUNTERS.clear();
    }
}
