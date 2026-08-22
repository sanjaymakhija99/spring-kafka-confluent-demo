package com.example.kafkademo.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Producer-side Kafka configuration.
 *
 * <p>This class builds the {@link ProducerFactory} / {@link KafkaTemplate}
 * beans used to publish messages to Confluent Cloud, and documents the
 * most important Kafka producer features/settings:
 *
 * <ul>
 *   <li><b>bootstrap.servers</b> - Confluent Cloud broker endpoint
 *       (e.g. {@code pkc-xxxxx.region.provider.confluent.cloud:9092}).</li>
 *   <li><b>SASL_SSL / PLAIN</b> - Confluent Cloud requires authenticated,
 *       encrypted connections using an API Key (username) and API Secret
 *       (password) issued from the Confluent Cloud console.</li>
 *   <li><b>acks=all</b> - the producer waits for the leader AND all
 *       in-sync replicas to acknowledge the write, giving the strongest
 *       durability guarantee.</li>
 *   <li><b>enable.idempotence=true</b> - prevents duplicate messages
 *       being written to a partition if the producer retries a send
 *       (e.g. due to a transient network blip). Combined with acks=all
 *       this gives "exactly-once per partition" write semantics.</li>
 *   <li><b>retries</b> - number of times to retry a failed send before
 *       giving up (transient errors are retried automatically).</li>
 *   <li><b>linger.ms / batch.size</b> - control batching: the producer
 *       waits up to {@code linger.ms} to accumulate more records into a
 *       batch (up to {@code batch.size} bytes) before sending, which
 *       greatly improves throughput at the cost of a small added latency.</li>
 *   <li><b>compression.type=snappy</b> - compresses batches before
 *       sending, reducing network bandwidth and storage on the brokers.</li>
 * </ul>
 */
@Configuration
public class KafkaProducerConfig {

    /** Confluent Cloud bootstrap server(s), e.g. pkc-xxxxx.region.aws.confluent.cloud:9092 */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /** Confluent Cloud API Key - used as the SASL username. */
    @Value("${spring.kafka.properties.sasl.api-key}")
    private String apiKey;

    /** Confluent Cloud API Secret - used as the SASL password. */
    @Value("${spring.kafka.properties.sasl.api-secret}")
    private String apiSecret;

    /**
     * Builds the low-level {@link ProducerFactory} with all producer
     * settings, including the SASL_SSL security configuration required
     * to connect to Confluent Cloud.
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> config = new HashMap<>();

        // --- Connection ---
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // --- Serialization: how Java objects are turned into bytes on the wire ---
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // --- Confluent Cloud security: SASL_SSL with PLAIN mechanism using API key/secret ---
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.mechanism", "PLAIN");
        config.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + apiKey + "\" password=\"" + apiSecret + "\";");

        // --- Reliability / delivery guarantees ---
        config.put(ProducerConfig.ACKS_CONFIG, "all");              // wait for all in-sync replicas
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // avoid duplicate writes on retry
        config.put(ProducerConfig.RETRIES_CONFIG, 5);               // retry transient failures

        // --- Performance tuning ---
        config.put(ProducerConfig.LINGER_MS_CONFIG, 20);            // batch up to 20ms worth of records
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);    // 32 KB batch size
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // compress batches

        return new DefaultKafkaProducerFactory<>(config);
    }

    /**
     * {@link KafkaTemplate} is Spring's convenient wrapper around the
     * native Kafka {@code Producer} - it's what application code
     * (see {@code MessageProducer}) actually calls to send records.
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
