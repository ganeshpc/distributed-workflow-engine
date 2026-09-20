package com.workflowengine.activity.stub;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test hook that parks a stub inside {@code execute} until {@link #release}.
 *
 * <p>Used to prove commit-before-invoke: POST can return the admit snapshot while the first
 * stub is still blocked, and a second connection sees {@code RUNNING}.
 * JVM-static; tests must {@link #clear()} so leftover latches do not stall
 * the next class. {@link #honor} runs on the executor thread.
 */
public final class ActivityBlockHook {

    private static final ConcurrentHashMap<String, Gate> GATES = new ConcurrentHashMap<>();

    private ActivityBlockHook() {
    }

    /**
     * Arms a gate for {@code activityName}.
     *
     * @param activityName stub name
     */
    public static void install(String activityName) {
        GATES.put(activityName, new Gate(new CountDownLatch(1), new CountDownLatch(1)));
    }

    /**
     * Waits until the stub has entered {@link #honor}.
     *
     * @param activityName stub name
     * @param timeout how long the test thread waits
     * @return false on timeout
     * @throws InterruptedException if the test thread is interrupted
     * @throws IllegalStateException if {@link #install} was not called
     */
    public static boolean awaitBlocked(String activityName, Duration timeout) throws InterruptedException {
        Gate gate = GATES.get(activityName);
        if (gate == null) {
            throw new IllegalStateException("no blocker installed for " + activityName);
        }
        return gate.entered.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Lets the blocked stub continue.
     *
     * @param activityName stub name
     */
    public static void release(String activityName) {
        Gate gate = GATES.get(activityName);
        if (gate != null) {
            gate.release.countDown();
        }
    }

    /**
     * Called from the stub: signals entered, then waits for {@link #release}.
     *
     * @param activityName stub name
     */
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

    /** Releases every gate and drops them. Safe to call when none are installed. */
    public static void clear() {
        for (Gate gate : GATES.values()) {
            gate.release.countDown();
        }
        GATES.clear();
    }

    /**
     * Pair of latches: {@code entered} for the test thread, {@code release} for the stub.
     *
     * @param entered counted down when honor starts
     * @param release counted down by {@link #release(String)}
     */
    private record Gate(CountDownLatch entered, CountDownLatch release) {
    }
}
