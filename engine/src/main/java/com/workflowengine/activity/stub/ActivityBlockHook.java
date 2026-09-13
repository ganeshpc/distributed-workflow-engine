package com.workflowengine.activity.stub;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class ActivityBlockHook {

    private static final ConcurrentHashMap<String, Gate> GATES = new ConcurrentHashMap<>();

    private ActivityBlockHook() {
    }

    public static void install(String activityName) {
        GATES.put(activityName, new Gate(new CountDownLatch(1), new CountDownLatch(1)));
    }

    public static boolean awaitBlocked(String activityName, Duration timeout) throws InterruptedException {
        Gate gate = GATES.get(activityName);
        if (gate == null) {
            throw new IllegalStateException("no blocker installed for " + activityName);
        }
        return gate.entered.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static void release(String activityName) {
        Gate gate = GATES.get(activityName);
        if (gate != null) {
            gate.release.countDown();
        }
    }

    public static void honor(String activityName) {
        Gate gate = GATES.get(activityName);
        if (gate == null) {
            return;
        }
        gate.entered.countDown();
        boolean interrupted = false;
        while (true) {
            try {
                gate.release.await();
                break;
            } catch (InterruptedException ex) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public static void clear() {
        for (Gate gate : GATES.values()) {
            gate.release.countDown();
        }
        GATES.clear();
    }

    private record Gate(CountDownLatch entered, CountDownLatch release) {
    }
}
