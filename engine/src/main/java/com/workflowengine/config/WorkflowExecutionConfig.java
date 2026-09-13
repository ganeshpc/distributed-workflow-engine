package com.workflowengine.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class WorkflowExecutionConfig {

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
