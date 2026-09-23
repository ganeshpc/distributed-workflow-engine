package com.workflowengine.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;

/**
 * Skips a Kafka record that is not an activity result.
 *
 * <p>The listener method receives {@code ActivityCompletion} only after
 * Jackson has deserialized the value. A value that is not that record is not
 * retried. Any other listener failure uses the container's default backoff,
 * then this handler logs it and commits the offset.
 *
 * <p>Process-wide singleton. Runs on the Kafka listener thread.
 */
@Configuration
@Slf4j
public class KafkaRecordErrorConfig {

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
