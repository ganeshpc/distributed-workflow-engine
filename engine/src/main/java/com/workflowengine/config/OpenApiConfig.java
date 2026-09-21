package com.workflowengine.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI spec and Scalar UI for the HTTP surface.
 *
 * <p>Scalar (cleaner than Swagger UI) is at {@code /scalar}. Spec is at
 * {@code /v3/api-docs}. Localhost only; no auth. Try-it-out calls the live
 * engine on this process.
 *
 * <p>Spring singleton. Does not change admit-then-run, recovery, or timeouts.
 */
@Configuration
public class OpenApiConfig {

    /**
     * API title and description shown in Scalar.
     *
     * @return OpenAPI info model
     */
    @Bean
    public OpenAPI workflowEngineOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Distributed Workflow Engine")
                .version("0.1.0")
                .description("""
                        Admit-then-run ORDER workflows. POST commits a PENDING instance \
                        and returns that admit snapshot (version 0); poll GET until \
                        COMPLETED or FAILED. Idempotent POST with the same key returns 200 \
                        and does not start a second run. Phase 2 recovery resumes leftovers \
                        after crash. A step that stays RUNNING past its deadline fails \
                        with error TIMED_OUT. FAILED is not retried."""));
    }
}
