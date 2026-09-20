package com.workflowengine.api.activity;

/**
 * One named business operation the engine can schedule as a step.
 *
 * <p>Implementations belong to workers. Phase 1 hosts them as in-process
 * stubs. They must not throw for expected business failure: return
 * {@link ActivityResult#success()} {@code false}. The invoker still catches
 * unexpected runtime exceptions and converts them.
 *
 * <p>Thread safety is the implementer's problem. Stubs may be invoked from
 * {@code workflow-*} executor threads. Do not assume a single thread.
 */
public interface Activity {

    /**
     * Returns the step name this activity handles, for example {@code CREATE_ORDER}.
     *
     * @return non-blank name matching a {@code StepDefinition}
     */
    String name();

    /**
     * Runs the operation. Must not be called inside an open workflow transaction.
     *
     * @param context engine-supplied identity and input; never null
     * @return success or failure payload; never null
     */
    ActivityResult execute(ActivityContext context);
}
