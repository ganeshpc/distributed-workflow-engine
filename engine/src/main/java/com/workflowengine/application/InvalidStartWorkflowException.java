package com.workflowengine.application;

public class InvalidStartWorkflowException extends RuntimeException {

    public InvalidStartWorkflowException(String message) {
        super(message);
    }
}
