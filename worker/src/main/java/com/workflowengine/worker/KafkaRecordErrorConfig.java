package com.workflowengine.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;

/**
 * Skips a Kafka record that is not an activity task.
 *
 * <p>{@link TaskListener} receives {@code ActivityContext} only after Jackson
 * has deserialized the value. A value that is not that record is not retried.
 * A failure while publishing a result uses the container's default backoff,
 * then this handler logs it and commits the offset.
 *
 * <p>Process-wide singleton. Runs on the Kafka listener thread.
 */
@Configuration
public class KafkaRecordErrorConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordErrorConfig.class);

    /**
     * @return error handler that does not retry a record that failed to deserialize
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler((record, exception) -> log.error(
                "discard record topic={} offset={}",
                record.topic(),
                record.offset(),
                exception));
    }
}
