package com.workflowengine.definition;

import com.workflowengine.domain.StepDefinition;
import com.workflowengine.domain.WorkflowDefinition;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public final class OrderWorkflowDefinition implements WorkflowDefinition {

    public static final String TYPE = "ORDER";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public int version() {
        return 1;
    }

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
