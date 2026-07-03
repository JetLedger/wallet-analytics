package com.jetledger.analytics.application.categorization;

import com.jetledger.contracts.CategorizationResult;
import com.jetledger.contracts.Category;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.math.BigDecimal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CategorizationService {

    private final ChatClient chatClient;
    private final double confidenceThreshold;
    private final ObservationRegistry observationRegistry;

    public CategorizationService(
            ChatClient.Builder chatClientBuilder,
            @Value("${categorization.confidence-threshold:0.5}") double confidenceThreshold,
            ObservationRegistry observationRegistry) {
        this.chatClient = chatClientBuilder.build();
        this.confidenceThreshold = confidenceThreshold;
        this.observationRegistry = observationRegistry;
    }

    public CategorizationResult categorize(String eventId, String type, BigDecimal amount, String currency) {
        return Observation.createNotStarted("categorization.classify", observationRegistry)
            .observe(() -> {
                long start = System.nanoTime();
                try {
                    CategoryResult response = chatClient.prompt()
                        .user(u -> u.text("""
                            Categorize this bank transaction into one of: FOOD, TRANSPORT, HEALTHCARE, \
                            ENTERTAINMENT, SHOPPING, UTILITIES, HOUSING, EDUCATION, SALARY, INCOME, \
                            TRANSFER, FEES, OTHER.
                            
                            Transaction type: {type}
                            Amount: {amount} {currency}
                            
                            Respond with a JSON object containing:
                            - category: the most appropriate category
                            - confidence: a confidence score between 0 and 1
                            - reasoning: brief explanation for the categorization
                            """)
                            .param("type", type)
                            .param("amount", amount.toPlainString())
                            .param("currency", currency))
                        .call()
                        .entity(CategoryResult.class);

                    Category category = toCategory(response.category());
                    boolean humanReview = response.confidence() < confidenceThreshold;

                    log.info("Categorized transaction {} as {} (confidence={}, humanReview={})",
                        eventId, category, response.confidence(), humanReview);

                    return new CategorizationResult(eventId, category, response.confidence(), response.reasoning(), humanReview);
                } finally {
                    long elapsed = System.nanoTime() - start;
                    log.debug("Categorization for {} took {}ms", eventId, elapsed / 1_000_000);
                }
            });
    }

    private Category toCategory(String name) {
        if (name == null) return Category.UNCATEGORIZED;
        try {
            return Category.valueOf(name.toUpperCase().trim());
        } catch (IllegalArgumentException e) {
            log.warn("Unknown category '{}' returned by AI, falling back to UNCATEGORIZED", name);
            return Category.UNCATEGORIZED;
        }
    }
}
