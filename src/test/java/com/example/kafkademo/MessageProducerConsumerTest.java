package com.example.kafkademo;

import com.example.kafkademo.consumer.MessageConsumer;
import com.example.kafkademo.producer.MessageProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Lightweight unit tests for the producer and consumer components.
 *
 * <p>These tests do NOT require a running Kafka broker (embedded or real):
 * the {@link KafkaTemplate} is mocked for the producer test, and the
 * consumer is exercised directly with a hand-built {@link ConsumerRecord}
 * and a mocked {@link Acknowledgment}. This keeps the test suite fast and
 * fully offline, while still verifying the core interaction contracts:
 * <ul>
 *   <li>{@code MessageProducer} calls {@code kafkaTemplate.send(topic, content)}.</li>
 *   <li>{@code MessageConsumer} acknowledges the record after processing it.</li>
 * </ul>
 */
class MessageProducerConsumerTest {

    private KafkaTemplate<String, String> kafkaTemplate;
    private MessageProducer messageProducer;
    private MessageConsumer messageConsumer;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);

        messageProducer = new MessageProducer(kafkaTemplate);
        // Inject the @Value-populated topic field directly, since we're not
        // loading a full Spring context in this lightweight unit test.
        ReflectionTestUtils.setField(messageProducer, "topicName", "demo-messages");

        messageConsumer = new MessageConsumer();
    }

    @Test
    void producerSendsMessageToConfiguredTopic() {
        // Arrange: make kafkaTemplate.send(...) return a completed future,
        // mimicking a successful broker acknowledgment.
        CompletableFuture<SendResult<String, String>> completedFuture = new CompletableFuture<>();
        completedFuture.complete(null);
        when(kafkaTemplate.send(anyString(), anyString())).thenReturn(completedFuture);

        // Act
        messageProducer.sendMessage("hello confluent cloud");

        // Assert: verify the producer published to the expected topic with the expected payload.
        verify(kafkaTemplate, times(1)).send("demo-messages", "hello confluent cloud");
    }

    @Test
    void consumerAcknowledgesAfterProcessing() {
        // Arrange
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("demo-messages", 0, 42L, "key-1", "hello confluent cloud");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        // Act
        messageConsumer.consume(record, acknowledgment);

        // Assert: the offset must be acknowledged exactly once after successful processing.
        verify(acknowledgment, times(1)).acknowledge();
    }
}
