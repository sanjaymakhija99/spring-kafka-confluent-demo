package com.example.kafkademo.controller;

import com.example.kafkademo.model.Message;
import com.example.kafkademo.producer.MessageProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Simple REST endpoint used to manually trigger publishing a message to
 * Kafka, e.g. for local testing with curl or Postman:
 *
 * <pre>
 * curl -X POST http://localhost:8080/api/messages \
 *      -H "Content-Type: application/json" \
 *      -d '{"content":"hello confluent cloud"}'
 * </pre>
 *
 * The message consumer ({@code MessageConsumer}) will pick up and log the
 * message shortly after it is produced.
 */
@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageProducer messageProducer;

    @PostMapping
    public ResponseEntity<String> publish(@Valid @RequestBody Message message) {
        messageProducer.sendMessage(message.getContent());
        return ResponseEntity.ok("Message accepted for publishing to Kafka: " + message.getContent());
    }
}
