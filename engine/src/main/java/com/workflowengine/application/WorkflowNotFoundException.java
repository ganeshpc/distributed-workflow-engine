package com.workflowengine.application;

import java.util.UUID;

/**
 * Missing instance on GET, mapped to HTTP 404 with a generic body.
 *
 * <p>The id is not echoed in the public error payload.
 */
public class WorkflowNotFoundException extends RuntimeException {

    /**
     * @param id requested workflow id; used by callers, not the HTTP body
     */
    public WorkflowNotFoundException(UUID id) {
        super("workflow not found");
    }
}
