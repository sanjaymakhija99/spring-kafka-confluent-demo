# Spring Kafka + Confluent Cloud Demo

@Author sanjay

A small, self-contained Spring Boot (Maven) project that shows how to
**produce** and **consume** messages using a **Confluent Cloud** managed
Kafka cluster, with important production-grade Kafka features enabled and
documented in the code.

## What this project demonstrates

| Area | Feature | Where |
|---|---|---|
| Producer | `acks=all`, idempotent producer, retries | `KafkaProducerConfig` |
| Producer | Batching (`linger.ms`, `batch.size`) + `snappy` compression | `KafkaProducerConfig` |
| Producer | Async send with success/failure callback | `MessageProducer` |
| Consumer | Consumer groups (`group.id`) for scalability/failover | `KafkaConsumerConfig` |
| Consumer | Manual offset commits (`AckMode.MANUAL_IMMEDIATE`) | `KafkaConsumerConfig`, `MessageConsumer` |
| Consumer | Concurrency (multiple listener threads) | `KafkaConsumerConfig` |
| Consumer | Error handling & retry with backoff (poison-pill protection) | `KafkaConsumerConfig` |
| Security | SASL_SSL / PLAIN auth against Confluent Cloud (API key/secret) | `KafkaProducerConfig`, `KafkaConsumerConfig` |
| REST | Simple endpoint to trigger a produce, for manual testing | `MessageController` |

## Project structure

```
spring-kafka-confluent-demo/
├── pom.xml
├── README.md
└── src
    ├── main
    │   ├── java/com/example/kafkademo/
    │   │   ├── KafkaDemoApplication.java       # Spring Boot entry point
    │   │   ├── config/
    │   │   │   ├── KafkaProducerConfig.java    # Producer factory + KafkaTemplate
    │   │   │   └── KafkaConsumerConfig.java    # Consumer factory + listener container
    │   │   ├── producer/MessageProducer.java   # Publishes messages
    │   │   ├── consumer/MessageConsumer.java   # @KafkaListener that consumes messages
    │   │   ├── controller/MessageController.java # REST endpoint to trigger a send
    │   │   └── model/Message.java              # Request DTO
    │   └── resources/application.yml           # Configuration (env-var driven)
    └── test/java/com/example/kafkademo/
        └── MessageProducerConsumerTest.java    # Embedded-Kafka round-trip test
```

## Prerequisites

1. **Java 17+** and **Maven 3.8+** installed.
2. A **Confluent Cloud** account with:
   - A cluster created (any tier, e.g. the free "Basic" cluster works fine).
   - A **topic** created, e.g. `demo-messages` (Confluent Cloud console →
     your cluster → Topics → Create topic).
   - An **API key/secret** generated for that cluster (Confluent Cloud
     console → your cluster → API Keys → Create key). This gives you the
     `SASL` username (API key) and password (API secret).
   - Your cluster's **bootstrap server** endpoint, shown on the cluster's
     "Cluster settings" page, e.g.
     `pkc-xxxxx.us-west-2.aws.confluent.cloud:9092`.

## Configuration

All connection details are supplied via **environment variables** (no
secrets are hardcoded in the repo). Set these before running the app:

```bash
export KAFKA_BOOTSTRAP_SERVERS="***:9092"
export KAFKA_API_KEY="your-confluent-cloud-api-key"
export KAFKA_API_SECRET="your-confluent-cloud-api-secret"
export KAFKA_TOPIC="demo-messages"
export KAFKA_CONSUMER_GROUP_ID="spring-kafka-demo-group"
```

These map to `src/main/resources/application.yml`, which has safe local
defaults if the variables aren't set (so unit tests still run without any
external configuration).

## Build

```bash
mvn clean package
```

This compiles the code, runs the embedded-Kafka test
(`MessageProducerConsumerTest`, which spins up an **in-memory** broker so
it does **not** need your Confluent Cloud credentials), and produces
`target/spring-kafka-confluent-demo.jar`.

## Run

```bash
mvn spring-boot:run
```

or, after packaging:

```bash
java -jar target/spring-kafka-confluent-demo.jar
```

The app starts an HTTP server on port `8080` and immediately starts a
Kafka consumer (3 concurrent threads, see `KafkaConsumerConfig`) listening
on the configured topic.

## Try it out

Publish a message via the REST endpoint:

```bash
curl -X POST http://localhost:8080/api/messages \
     -H "Content-Type: application/json" \
     -d '{"content":"hello confluent cloud"}'
```

You should see log output similar to:

```
Publishing message to topic 'demo-messages': hello confluent cloud
Message sent successfully to partition=0, offset=42
Consumed message -> partition=0, offset=42, key=null, value=hello confluent cloud
```

The first two lines come from `MessageProducer`, the third from
`MessageConsumer` — confirming the full produce → Confluent Cloud →
consume round trip.

## Notes on the important Kafka features used

- **Idempotent producer + `acks=all`**: guarantees a message is written
  exactly once per partition even if the producer has to retry a send,
  and that it's durably replicated before being acknowledged.
- **Manual consumer acknowledgment**: the consumer only commits its
  offset after successfully processing a record, so a crash mid-processing
  results in the message being redelivered rather than lost.
- **Consumer groups & concurrency**: multiple consumer threads/instances
  sharing a `group.id` let you scale message processing horizontally;
  Kafka automatically rebalances partitions across available consumers.
- **Error handling with backoff**: a `DefaultErrorHandler` retries failed
  records a bounded number of times before giving up, preventing a single
  malformed ("poison pill") message from blocking an entire partition.

## Troubleshooting

- **`SaslAuthenticationException`**: double-check `KAFKA_API_KEY` /
  `KAFKA_API_SECRET` are correct and that the API key belongs to the same
  cluster referenced by `KAFKA_BOOTSTRAP_SERVERS`.
- **`UnknownTopicOrPartitionException`**: make sure the topic named in
  `KAFKA_TOPIC` actually exists in your Confluent Cloud cluster.
- **Connection timeouts**: verify your network/firewall allows outbound
  traffic to Confluent Cloud on port 9092, and that the bootstrap server
  hostname was copied exactly from the console.
