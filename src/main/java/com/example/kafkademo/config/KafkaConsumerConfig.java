package com.example.kafkademo.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer-side Kafka configuration.
 *
 * <p>Documents and configures the most important Kafka consumer features:
 *
 * <ul>
 *   <li><b>group.id</b> - the consumer group name. Kafka distributes the
 *       topic's partitions across all consumer instances sharing the same
 *       group id, enabling horizontal scaling and automatic failover.</li>
 *   <li><b>auto.offset.reset=earliest</b> - if no committed offset exists
 *       yet for this group (e.g. first run), start reading from the
 *       beginning of the topic rather than only new messages.</li>
 *   <li><b>enable.auto.commit=false</b> + <b>AckMode.MANUAL_IMMEDIATE</b> -
 *       we disable Kafka's automatic offset commits and instead commit
 *       offsets manually only AFTER the message has been successfully
 *       processed. This avoids losing messages if the app crashes mid
 *       processing (at-least-once delivery).</li>
 *   <li><b>concurrency</b> - number of listener threads/consumer
 *       instances within this JVM; Spring Kafka will spin up this many
 *       consumers in the group, each handling a subset of partitions,
 *       for parallel processing.</li>
 *   <li><b>DefaultErrorHandler with FixedBackOff</b> - if message
 *       processing throws an exception, the record is retried a fixed
 *       number of times (with a delay) before being logged/skipped,
 *       preventing a single bad ("poison pill") message from blocking
 *       the whole partition forever.</li>
 * </ul>
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.sasl.api-key}")
    private String apiKey;

    @Value("${spring.kafka.properties.sasl.api-secret}")
    private String apiSecret;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    /**
     * Builds the low-level {@link ConsumerFactory} with connection,
     * deserialization, security, and offset-handling settings.
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();

        // --- Connection ---
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // --- Deserialization: turning bytes on the wire back into Java objects ---
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // --- Confluent Cloud security: same SASL_SSL/PLAIN mechanism as the producer ---
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.mechanism", "PLAIN");
        config.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + apiKey + "\" password=\"" + apiSecret + "\";");

        // --- Offset management ---
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // read from start if no offset committed
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);      // we commit manually after processing

        // --- Reliability tuning ---
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); // max records returned per poll() call

        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * The listener container factory wires together the {@link ConsumerFactory}
     * with concurrency and error-handling behaviour, and is referenced by
     * {@code @KafkaListener(containerFactory = "kafkaListenerContainerFactory")}.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // Run 3 concurrent consumer threads in this JVM (bounded by the
        // topic's partition count - extra threads beyond partition count sit idle).
        factory.setConcurrency(3);

        // MANUAL_IMMEDIATE: the application must call Acknowledgment.acknowledge()
        // itself after successfully processing a record; the offset is then
        // committed to Kafka right away.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Error handling: if the listener method throws, retry the same
        // record up to 3 times with a 1-second pause between attempts.
        // After retries are exhausted, the error is logged and the
        // consumer moves on (preventing a single bad message from
        // blocking the partition indefinitely).
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 3)));

        return factory;
    }
}
