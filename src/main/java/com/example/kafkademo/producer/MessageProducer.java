package com.example.kafkademo.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Service responsible for publishing messages to the configured Kafka topic
 * on Confluent Cloud.
 *
 * <p>Uses {@link KafkaTemplate#send(String, Object)}, which is asynchronous
 * and returns a {@link CompletableFuture}. We attach a callback so we can
 * log success (partition/offset the record landed on) or failure without
 * blocking the calling thread - this is the recommended non-blocking
 * pattern for high-throughput producers.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /** Topic name is externalised to application.yml so it's easy to change per environment. */
    @Value("${app.kafka.topic}")
    private String topicName;

    /**
     * Publishes the given content to the configured topic.
     *
     * @param content the message payload to send
     */
    public void sendMessage(String content) {
        log.info("Publishing message to topic '{}': {}", topicName, content);

        // send() is asynchronous - it returns immediately with a Future.
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, content);

        // Attach a non-blocking callback to observe the outcome once the
        // broker has acknowledged (or rejected) the record.
        future.whenComplete((result, exception) -> {
            if (exception == null) {
                log.info("Message sent successfully to partition={}, offset={}",
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send message: {}", exception.getMessage(), exception);
            }
        });
    }
}
