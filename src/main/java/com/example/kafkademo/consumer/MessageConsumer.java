package com.example.kafkademo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Listens for messages on the configured Kafka topic (same topic the
 * {@code MessageProducer} publishes to) and processes them.
 *
 * <p>Important feature demonstrated here: <b>manual acknowledgment</b>.
 * Because {@code KafkaConsumerConfig} sets {@code AckMode.MANUAL_IMMEDIATE}
 * and disables auto-commit, this listener must explicitly call
 * {@link Acknowledgment#acknowledge()} once it has finished processing a
 * record. Only then does Spring Kafka commit the offset back to Confluent
 * Cloud. This guarantees that if the application crashes WHILE processing
 * a message (before acknowledge() is called), the message will be
 * re-delivered on restart instead of being silently lost.
 */
@Slf4j
@Component
public class MessageConsumer {

    /**
     * @param record the full Kafka consumer record (gives access to key,
     *               value, partition, offset, timestamp, headers, etc.)
     * @param acknowledgment used to manually commit the offset after
     *               successful processing
     */
    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {

            log.info("Consumed message -> partition={}, offset={}, key={}, value={}",
                    record.partition(), record.offset(), record.key(), record.value());

            // ---- Business processing would happen here ----
            // e.g. persist to a database, trigger a downstream workflow, etc.

            // Only commit the offset once processing has completed successfully.
            acknowledgment.acknowledge();

        } catch (Exception ex) {
            // If processing fails, we deliberately do NOT acknowledge.
            // Re-throwing lets the DefaultErrorHandler (configured in
            // KafkaConsumerConfig) apply the retry/backoff policy.
            log.error("Error processing record at offset {}: {}", record.offset(), ex.getMessage());
            throw ex;
        }
    }
}
