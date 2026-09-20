package com.workflowengine.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Clock;

/**
 * Executor used to run {@link com.workflowengine.runtime.WorkflowExecutor}
 * after the admit transaction commits.
 *
 * <p>Daemon threads, prefix {@code workflow-}. Shutdown does not wait for
 * in-flight sagas: leftovers stay in Postgres for the Phase 2 scanner.
 * Enables scheduling for {@link com.workflowengine.runtime.RecoveryScanner}.
 */
@Configuration
@EnableScheduling
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

    /**
     * Clock used for {@code next_attempt_at} due checks. Tests may replace this bean.
     *
     * @return UTC system clock
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * Daemon scheduler so recovery ticks do not keep the JVM alive after tests
     * or {@code spring-boot:run} shutdown.
     *
     * @return single-thread scheduler named {@code recovery-}
     */
    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("recovery-");
        scheduler.setDaemon(true);
        scheduler.initialize();
        return scheduler;
    }
}
