package com.jetledger.analytics;

import com.jetledger.analytics.domain.Transaction;
import com.jetledger.analytics.domain.TransactionRepository;
import com.jetledger.analytics.infrastructure.consumer.CloudEvent;
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
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "analytics.kafka.topic=wallet.transactions.v1"
})
@EmbeddedKafka(topics = { "wallet.transactions.v1" }, partitions = 1)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class TransactionEventConsumerTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private TransactionRepository transactionRepository;

    private KafkaTemplate<String, String> kafkaTemplate;
    private Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> producerProps = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));

        Map<String, Object> consumerProps = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString(),
            ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
        );
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps);
        embeddedKafka.consumeFromEmbeddedTopics(consumer, "wallet.transactions.v1");
        consumer.poll(Duration.ofMillis(500));
        consumer.seekToBeginning(consumer.assignment());
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) consumer.close();
    }

    @Test
    void shouldConsumeAndPersistDepositEvent() {
        String eventId = UUID.randomUUID().toString();
        String walletId = UUID.randomUUID().toString();
        String json = """
            {"specversion":"1.0","type":"com.jetledger.wallet.deposited","source":"/wallet-core","id":"%s","time":"2026-06-21T12:00:00Z","datacontenttype":"application/json","data":{"walletId":"%s","amount":"100.00","currency":"USD","balanceAfter":"100.00","correlationId":"%s","timestamp":"2026-06-21T12:00:00Z"}}
            """.formatted(eventId, walletId, UUID.randomUUID().toString());

        kafkaTemplate.send("wallet.transactions.v1", eventId, json);

        var record = KafkaTestUtils.getSingleRecord(consumer, "wallet.transactions.v1", Duration.ofSeconds(10));
        assertEquals(eventId, record.key());

        var handler = new TransactionEventConsumer(transactionRepository);
        handler.consume(record, null);

        assertTrue(transactionRepository.existsByEventId(eventId));
        Transaction tx = transactionRepository.findAll().stream()
            .filter(t -> t.getEventId().equals(eventId))
            .findFirst().orElseThrow();
        assertEquals("DEPOSIT", tx.getType());
        assertEquals(UUID.fromString(walletId), tx.getWalletId());
        assertEquals(new BigDecimal("100.00"), tx.getAmount());
    }

    @Test
    void shouldHandleDuplicateEventsIdempotently() {
        String eventId = UUID.randomUUID().toString();
        String walletId = UUID.randomUUID().toString();
        String json = """
            {"specversion":"1.0","type":"com.jetledger.wallet.deposited","source":"/wallet-core","id":"%s","time":"2026-06-21T12:00:00Z","datacontenttype":"application/json","data":{"walletId":"%s","amount":"100.00","currency":"USD","balanceAfter":"100.00","correlationId":"%s","timestamp":"2026-06-21T12:00:00Z"}}
            """.formatted(eventId, walletId, UUID.randomUUID().toString());

        kafkaTemplate.send("wallet.transactions.v1", eventId, json);

        var record1 = KafkaTestUtils.getSingleRecord(consumer, "wallet.transactions.v1", Duration.ofSeconds(10));
        var handler = new TransactionEventConsumer(transactionRepository);

        handler.consume(record1, null);
        assertEquals(1, transactionRepository.findAll().stream()
            .filter(t -> t.getEventId().equals(eventId)).count());

        handler.consume(record1, null);
        assertEquals(1, transactionRepository.findAll().stream()
            .filter(t -> t.getEventId().equals(eventId)).count());
    }
}
