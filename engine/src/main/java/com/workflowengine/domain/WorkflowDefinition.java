package com.workflowengine.domain;

import java.util.List;

public interface WorkflowDefinition {

    String type();

    int version();

    List<StepDefinition> steps();
}
