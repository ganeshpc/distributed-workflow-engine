package com.workflowengine.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Executor used to run {@link com.workflowengine.runtime.WorkflowExecutor}
 * after TX1 commits.
 *
 * <p>Daemon threads, prefix {@code workflow-}. Shutdown does not wait for
 * in-flight sagas: leftovers stay in Postgres for Phase 2. This bean is a
 * process singleton; many request threads submit work to it.
 */
@Configuration
public class WorkflowExecutionConfig {

    /**
     * Builds the pool admission submits to.
     *
     * @return initialized {@code TaskExecutor} named {@code workflowTaskExecutor}
     */
    @Bean
    public TaskExecutor workflowTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("workflow-");
        executor.setWaitForTasksToCompleteOnShutdown(false);
        executor.setDaemon(true);
        executor.initialize();
        return executor;
    }
}
