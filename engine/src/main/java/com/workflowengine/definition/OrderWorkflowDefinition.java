package com.workflowengine.definition;

import com.workflowengine.domain.StepDefinition;
import com.workflowengine.domain.WorkflowDefinition;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Linear Phase 1 {@code ORDER} saga: create order, reserve inventory, pay,
 * ship, notify.
 *
 * <p>Version is 1. Step names match in-process stub {@code Activity#name()}
 * values. The graph is immutable and a Spring singleton.
 */
@Component
public final class OrderWorkflowDefinition implements WorkflowDefinition {

    /** Client {@code type} value for this graph. */
    public static final String TYPE = "ORDER";

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
                new StepDefinition("CREATE_ORDER"),
                new StepDefinition("RESERVE_INVENTORY"),
                new StepDefinition("PROCESS_PAYMENT"),
                new StepDefinition("CREATE_SHIPMENT"),
                new StepDefinition("SEND_NOTIFICATION")
        );
    }
}
