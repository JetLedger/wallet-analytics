package com.jetledger.analytics.infrastructure.categorization;

import com.jetledger.contracts.CategorizationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
public class CategorizationResultPublisher {

    private static final String SPEC_VERSION = "1.0";
    private static final String SOURCE = "/wallet-analytics";
    private static final String CONTENT_TYPE = "application/json";
    private static final String EVENT_TYPE = "com.jetledger.transaction.categorized";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public CategorizationResultPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${categorization.kafka.topic:wallet.categorizations.v1}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publish(CategorizationResult result) {
        try {
            ObjectNode dataNode = MAPPER.createObjectNode();
            dataNode.put("eventId", result.eventId());
            dataNode.put("category", result.category().name());
            dataNode.put("confidence", result.confidence());
            dataNode.put("reasoning", result.reasoning());
            dataNode.put("humanReviewRequired", result.humanReviewRequired());

            ObjectNode root = MAPPER.createObjectNode();
            root.put("specversion", SPEC_VERSION);
            root.put("type", EVENT_TYPE);
            root.put("source", SOURCE);
            root.put("id", UUID.randomUUID().toString());
            root.put("time", Instant.now().toString());
            root.put("datacontenttype", CONTENT_TYPE);
            root.set("data", dataNode);

            String json = MAPPER.writeValueAsString(root);
            kafkaTemplate.send(topic, result.eventId(), json)
                .whenComplete((metadata, ex) -> {
                    if (ex == null) {
                        log.info("Published categorization for event {} to {} at offset {}",
                            result.eventId(), topic, metadata.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish categorization for event {} to {}: {}",
                            result.eventId(), topic, ex.getMessage(), ex);
                    }
                });
        } catch (Exception e) {
            log.error("Failed to serialize categorization result for event {}: {}",
                result.eventId(), e.getMessage(), e);
        }
    }
}
