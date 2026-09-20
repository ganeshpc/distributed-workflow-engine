package com.workflowengine.api.activity;

/**
 * Outcome of one activity invoke. The executor treats {@code success == false}
 * as the step-fail transaction; it never infers failure from a thrown exception because
 * the invoker already converted those.
 *
 * @param success true if the step should become {@code COMPLETED}
 * @param outputJson optional JSON text stored on the step
 * @param error required when {@code success} is false; copied onto instance and step
 */
public record ActivityResult(
        boolean success,
        String outputJson,
        String error
) {
}
