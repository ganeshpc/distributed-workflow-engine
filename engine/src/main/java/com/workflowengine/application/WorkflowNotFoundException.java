package com.workflowengine.application;

import java.util.UUID;

public class WorkflowNotFoundException extends RuntimeException {

    public WorkflowNotFoundException(UUID id) {
        super("workflow not found");
    }
}
