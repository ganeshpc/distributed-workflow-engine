package com.workflowengine.definition;

import com.workflowengine.domain.RetryPolicy;
import com.workflowengine.domain.StepDefinition;
import com.workflowengine.domain.WorkflowDefinition;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Linear {@code ORDER} saga: create order, reserve inventory, pay, ship, notify.
 *
 * <p>Version is 1. Step names match in-process stub {@code Activity#name()}
 * values. Each step uses {@link RetryPolicy#defaults()} and
 * {@link #STEP_TIMEOUT}. The graph is immutable and a Spring singleton.
 *
 * <p>No failure handling of its own. The executor and the timeout poller
 * read this list; they do not modify it.
 */
@Component
public final class OrderWorkflowDefinition implements WorkflowDefinition {

    /** Client {@code type} value for this graph. */
    public static final String TYPE = "ORDER";

    /**
     * Per-attempt deadline for every ORDER step. Long enough that an in-process
     * stub finishes; tests that need an earlier deadline move the engine
     * {@code Clock} instead of shrinking this constant.
     */
    public static final Duration STEP_TIMEOUT = Duration.ofMinutes(5);

    /** {@inheritDoc} */
    @Override
    public String type() {
        return TYPE;
    }

    /** {@inheritDoc} */
    @Override
    public int version() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override
    public List<StepDefinition> steps() {
        return List.of(
                step("CREATE_ORDER"),
                step("RESERVE_INVENTORY"),
                step("PROCESS_PAYMENT"),
                step("CREATE_SHIPMENT"),
                step("SEND_NOTIFICATION")
        );
    }

    /**
     * ORDER step with the shared retry policy and {@link #STEP_TIMEOUT}.
     *
     * @param name activity name
     * @return immutable step definition
     */
    private static StepDefinition step(String name) {
        return new StepDefinition(name, RetryPolicy.defaults(), STEP_TIMEOUT);
    }
}
