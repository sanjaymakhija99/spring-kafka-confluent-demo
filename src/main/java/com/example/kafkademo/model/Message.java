package com.example.kafkademo.model;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Simple payload object used as the request body for the REST endpoint
 * that triggers publishing a message to Kafka.
 *
 * Kept intentionally simple (a single "content" field) since the goal
 * of this demo is to showcase Kafka producer/consumer wiring, not a
 * complex domain model.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message {

    /** The text content that will be sent as the Kafka record value. */
    @NotBlank(message = "content must not be blank")
    private String content;
}
