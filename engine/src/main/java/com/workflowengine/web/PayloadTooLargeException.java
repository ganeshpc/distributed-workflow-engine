package com.workflowengine.web;

public class PayloadTooLargeException extends RuntimeException {

    public PayloadTooLargeException() {
        super("payload too large");
    }
}
