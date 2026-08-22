package com.example.kafkademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of the Spring Boot application.
 *
 * <p>This app demonstrates:
 * <ul>
 *   <li>Producing messages to a topic on Confluent Cloud (managed Kafka).</li>
 *   <li>Consuming messages from the same topic using a {@code @KafkaListener}.</li>
 *   <li>Important production-grade Kafka features such as: idempotent
 *       producers, acknowledgement/retry configuration, manual consumer
 *       offset commits, concurrency, and error handling.</li>
 * </ul>
 *
 * Run with: {@code mvn spring-boot:run}
 * (after configuring your Confluent Cloud credentials in application.yml
 * or via environment variables - see README.md).
 */
@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }
}
