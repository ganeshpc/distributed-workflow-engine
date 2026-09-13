package com.workflowengine.api.activity;

public record ActivityResult(
        boolean success,
        String outputJson,
        String error
) {
}
