package com.workflowengine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Boot entry point for the Phase 1 single-process engine.
 *
 * <p>Scans {@code com.workflowengine} for web, admission, executor, stubs, and
 * persistence. Listens on port 8080. Actuator exposes {@code health} only.
 *
 * <p>This class is a process singleton created by Spring. Failure to start
 * (missing Postgres, Flyway error) is a JVM exit, not an HTTP mapping.
 */
@SpringBootApplication
public class WorkflowEngineApplication {

    /**
     * Starts the Spring context.
     *
     * @param args unused; configuration is {@code application.yml} and the environment
     */
    public static void main(String[] args) {
        SpringApplication.run(WorkflowEngineApplication.class, args);
    }
}
