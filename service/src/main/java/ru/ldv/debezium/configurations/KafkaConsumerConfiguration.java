package ru.ldv.debezium.configurations;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaConsumerConfiguration {

    @Value("${cdc.retry.max-attempts:3}")
    private int maxAttempts;

    @Value("${cdc.retry.backoff-interval-ms:2000}")
    private long backoffInterval;

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
            log.error(
                    "All retry attempts failed for record with key '{}' from topic '{}'. Sending to dlt...",
                    record.key(), record.topic(), ex
            );

            return new TopicPartition(record.topic() + "-dlt", record.partition());
        });

        FixedBackOff backOff = new FixedBackOff(backoffInterval, maxAttempts - 1);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Исключения, при которых НЕ нужно делать Retry
         errorHandler.addNotRetryableExceptions(NullPointerException.class);

        return errorHandler;
    }
}
