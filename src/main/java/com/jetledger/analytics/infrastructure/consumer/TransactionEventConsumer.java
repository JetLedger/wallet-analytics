package com.jetledger.analytics.infrastructure.consumer;

import com.jetledger.analytics.domain.Transaction;
import com.jetledger.analytics.domain.TransactionRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Component
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;

    public TransactionEventConsumer(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    @KafkaListener(topics = "${analytics.kafka.topic:wallet.transactions.v1}", groupId = "${spring.kafka.consumer.group-id:wallet-analytics}")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String json = record.value();
        String eventId = record.key();

        log.info("Received event: id={}, topic={}, partition={}, offset={}", eventId, record.topic(), record.partition(), record.offset());

        if (transactionRepository.existsByEventId(eventId)) {
            log.debug("Skipping duplicate event: id={}", eventId);
            if (ack != null) ack.acknowledge();
            return;
        }

        var cloudEventOpt = CloudEvent.fromJson(json);
        if (cloudEventOpt.isEmpty()) {
            log.warn("Failed to parse CloudEvent envelope: eventId={}", eventId);
            throw new IllegalArgumentException("Malformed CloudEvent envelope");
        }

        CloudEvent cloudEvent = cloudEventOpt.get();
        JsonNode data = cloudEvent.data();

        try {
            UUID walletId = UUID.fromString(data.get("walletId").asText());
            BigDecimal amount = new BigDecimal(data.get("amount").asText());
            String currency = data.has("currency") ? data.get("currency").asText() : "USD";
            BigDecimal balanceAfter = data.has("balanceAfter") ? new BigDecimal(data.get("balanceAfter").asText()) : null;
            String correlationId = data.has("correlationId") ? data.get("correlationId").asText() : null;
            java.time.Instant timestamp = java.time.Instant.parse(cloudEvent.time());

            String eventType = mapType(cloudEvent.type());

            Transaction transaction = new Transaction(
                UUID.randomUUID(), eventId, walletId, eventType, amount, currency,
                balanceAfter, correlationId, timestamp
            );

            transactionRepository.save(transaction);
            transactionRepository.flush();
            log.info("Persisted transaction: eventId={}, type={}, walletId={}, amount={} {}",
                eventId, eventType, walletId, amount, currency);

            if (ack != null) ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process event {}: {}", eventId, e.getMessage(), e);
            throw new IllegalArgumentException("Failed to process event: " + eventId, e);
        }
    }

    private String mapType(String cloudEventType) {
        return switch (cloudEventType) {
            case "com.jetledger.wallet.created" -> "WALLET_CREATED";
            case "com.jetledger.wallet.deposited" -> "DEPOSIT";
            case "com.jetledger.wallet.withdrawn" -> "WITHDRAWAL";
            case "com.jetledger.wallet.withdraw.rejected" -> "WITHDRAW_REJECTED";
            default -> cloudEventType;
        };
    }
}
