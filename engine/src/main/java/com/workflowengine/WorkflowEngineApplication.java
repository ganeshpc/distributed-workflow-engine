package com.workflowengine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Boot entry point for the engine process.
 *
 * <p>Scans the engine packages only. Activity stubs live in the worker
 * process and must not be component-scanned here, including when a test
 * classpath contains the worker jar. Listens on port 8080. Actuator exposes
 * {@code health} only.
 *
 * <p>Entity and repository scans stay on {@code com.workflowengine.persistence}.
 * The worker jar is on the engine test classpath and owns
 * {@code activity_completion} in a different database. Scanning
 * {@code com.workflowengine.worker} would map that table onto the engine
 * datasource.
 *
 * <p>This class is a process singleton created by Spring. Failure to start
 * (missing Postgres, Kafka, or Flyway) is a JVM exit, not an HTTP mapping.
 */
@SpringBootApplication(scanBasePackages = {
        "com.workflowengine.application",
        "com.workflowengine.config",
        "com.workflowengine.definition",
        "com.workflowengine.domain",
        "com.workflowengine.persistence",
        "com.workflowengine.runtime",
        "com.workflowengine.web"
})
@EntityScan("com.workflowengine.persistence")
@EnableJpaRepositories("com.workflowengine.persistence")
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
