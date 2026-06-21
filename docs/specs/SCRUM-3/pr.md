# [SCRUM-3] Publish domain events to Redpanda — wallet-analytics

## Changes
- New Spring Boot 4 service scaffolded with Web, JPA, Kafka, Actuator, Micrometer
- `AnalyticsApplication.java` — entry point
- `Transaction.java` — JPA read model entity with `event_id` unique constraint
- `TransactionRepository.java` — JPA repository with `existsByEventId` for idempotent dedup
- `KafkaConsumerConfiguration.java` — Kafka producer/consumer factories, `@EnableKafka`, DLQ error handler
- `TransactionEventConsumer.java` — `@KafkaListener` with at-least-once semantics, manual ack, idempotent handler
- `CloudEvent.java` — CloudEvents 1.0 parser with Jackson 3
- Micrometer consumer lag metric exposed via `/actuator/prometheus`
- Integration test with EmbeddedKafka: event consumption + duplicate idempotency
- Dead-letter topic `wallet.transactions.v1.dlq` for deserialization failures

## Cross-references
- wallet-contracts: https://github.com/JetLedger/wallet-contracts/pull/1
- wallet-core: https://github.com/JetLedger/wallet-core/pull/3

## Validation
- 2 tests pass (EmbeddedKafka integration tests)
- Built against Spring Boot 4 / Java 25
