package com.workflowengine.web;

/**
 * Generic HTTP error body. Values are fixed phrases such as {@code Bad Request};
 * never SQL or stack traces.
 *
 * @param error public message
 */
public record ErrorResponse(String error) {
}
