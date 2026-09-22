package com.workflowengine.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * One worker process for every ORDER activity.
 *
 * <p>Depends on {@code engine-api} only. It consumes {@code activity.tasks},
 * runs the stub, and publishes {@code activity.results}. It has no database
 * and does not decide the next step. Scan stays in this package so an engine
 * test classpath cannot pick these beans up inside the engine context.
 *
 * <p>Process singleton. A missing broker fails startup. Activity failures
 * become result messages; they do not crash the process.
 */
@SpringBootApplication(scanBasePackages = "com.workflowengine.worker")
public class WorkerApplication {

    /**
     * Starts the worker.
     *
     * @param args Spring Boot arguments, including the Kafka bootstrap servers
     */
    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }
}
