package com.workflowengine.web;

/**
 * Thrown when the body exceeds 65536 bytes. Mapped to HTTP 413 with a generic
 * body. May be thrown from the filter before the controller runs, including
 * while reading a chunked body.
 */
public class PayloadTooLargeException extends RuntimeException {

    /** Creates the filter/controller 413 signal. */
    public PayloadTooLargeException() {
        super("payload too large");
    }
}
