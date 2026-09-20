package com.workflowengine.application;

/**
 * Request-thread validation failure mapped to HTTP 400 with a generic body.
 *
 * <p>The message is for logs and tests, not for the JSON error payload.
 * Thrown only on the HTTP thread before or during TX1.
 */
public class InvalidStartWorkflowException extends RuntimeException {

    /**
     * @param message why the command was rejected; not returned to the client
     */
    public InvalidStartWorkflowException(String message) {
        super(message);
    }
}
