package com.jetledger.analytics;

import com.jetledger.analytics.config.TestCategorizationConfig;
import com.jetledger.analytics.domain.Transaction;
import com.jetledger.analytics.domain.TransactionRepository;
import com.jetledger.analytics.infrastructure.consumer.TransactionEventConsumer;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Import(TestCategorizationConfig.class)
@EmbeddedKafka(topics = { "wallet.transactions.v1", "wallet.transactions.v1.dlq" }, partitions = 1)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class TransactionEventConsumerTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private TransactionEventConsumer consumer;

    private KafkaTemplate<String, String> kafkaTemplate;
    private Consumer<String, String> consumerVerifier;

    @BeforeEach
    void setUp() {
        Map<String, Object> producerProps = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));

        Map<String, Object> verifierProps = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString(),
            ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + UUID.randomUUID(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
        );
        consumerVerifier = new org.apache.kafka.clients.consumer.KafkaConsumer<>(verifierProps);
        embeddedKafka.consumeFromEmbeddedTopics(consumerVerifier, "wallet.transactions.v1");
        consumerVerifier.poll(Duration.ofMillis(500));
        consumerVerifier.seekToBeginning(consumerVerifier.assignment());
    }

    @AfterEach
    void tearDown() {
        if (consumerVerifier != null) consumerVerifier.close();
    }

    @Test
    void shouldConsumeAndPersistDepositEvent() {
        String eventId = UUID.randomUUID().toString();
        String walletId = UUID.randomUUID().toString();
        String json = """
            {"specversion":"1.0","type":"com.jetledger.wallet.deposited","source":"/wallet-core","id":"%s","time":"2026-06-21T12:00:00Z","datacontenttype":"application/json","data":{"walletId":"%s","amount":"100.00","currency":"USD","balanceAfter":"100.00","correlationId":"%s","timestamp":"2026-06-21T12:00:00Z"}}
            """.formatted(eventId, walletId, UUID.randomUUID().toString());

        kafkaTemplate.send("wallet.transactions.v1", eventId, json);
        kafkaTemplate.flush();

        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumerVerifier, "wallet.transactions.v1", Duration.ofSeconds(10));
        assertEquals(eventId, record.key());

        consumer.consume(record, null);

        assertTrue(transactionRepository.existsByEventId(eventId));
        Transaction tx = transactionRepository.findAll().stream()
            .filter(t -> t.getEventId().equals(eventId))
            .findFirst().orElseThrow();
        assertEquals("DEPOSIT", tx.getType());
        assertEquals(UUID.fromString(walletId), tx.getWalletId());
        assertEquals(new BigDecimal("100.00"), tx.getAmount());
        assertTrue(tx.isCategorized());
        assertEquals("SALARY", tx.getCategory());
    }

    @Test
    void shouldHandleDuplicateEventsIdempotently() {
        String eventId = UUID.randomUUID().toString();
        String walletId = UUID.randomUUID().toString();
        String json = """
            {"specversion":"1.0","type":"com.jetledger.wallet.deposited","source":"/wallet-core","id":"%s","time":"2026-06-21T12:00:00Z","datacontenttype":"application/json","data":{"walletId":"%s","amount":"100.00","currency":"USD","balanceAfter":"100.00","correlationId":"%s","timestamp":"2026-06-21T12:00:00Z"}}
            """.formatted(eventId, walletId, UUID.randomUUID().toString());

        kafkaTemplate.send("wallet.transactions.v1", eventId, json);
        kafkaTemplate.flush();

        ConsumerRecord<String, String> record1 = KafkaTestUtils.getSingleRecord(consumerVerifier, "wallet.transactions.v1", Duration.ofSeconds(10));

        consumer.consume(record1, null);
        assertEquals(1, transactionRepository.findAll().stream()
            .filter(t -> t.getEventId().equals(eventId)).count());

        consumer.consume(record1, null);
        assertEquals(1, transactionRepository.findAll().stream()
            .filter(t -> t.getEventId().equals(eventId)).count());
    }

    @Test
    void shouldRejectMalformedEvent() {
        String malformed = "{not valid json";
        String eventId = "bad-key";

        assertThrows(IllegalArgumentException.class, () -> {
            consumer.consume(
                new ConsumerRecord<>("wallet.transactions.v1", 0, 0, eventId, malformed),
                null
            );
        });
        assertFalse(transactionRepository.existsByEventId(eventId));
    }
}
