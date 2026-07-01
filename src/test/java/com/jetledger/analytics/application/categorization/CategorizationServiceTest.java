package com.jetledger.analytics.application.categorization;

import com.jetledger.contracts.CategorizationResult;
import com.jetledger.contracts.Category;
import io.micrometer.observation.ObservationRegistry;
import java.math.BigDecimal;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.client.ChatClient;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CategorizationServiceTest {

    @Mock
    private ChatClient.Builder chatClientBuilder;

    @Mock
    private ChatClient chatClient;

    private CategorizationService service;

    @BeforeEach
    void setUp() {
        when(chatClientBuilder.build()).thenReturn(chatClient);
        service = new CategorizationService(chatClientBuilder, 0.5, ObservationRegistry.NOOP);
    }

    @Test
    void shouldReturnCategorizedResultWhenConfidenceAboveThreshold() {
        var requestSpec = mock(ChatClient.ChatClientRequestSpec.class);
        var responseSpec = mock(ChatClient.CallResponseSpec.class);

        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.user(any(Consumer.class))).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(responseSpec);
        when(responseSpec.entity(CategoryResult.class))
            .thenReturn(new CategoryResult("FOOD", 0.95, "Transaction at grocery store"));

        CategorizationResult result = service.categorize("evt-1", "WITHDRAWAL", new BigDecimal("50.00"), "USD");

        assertEquals(Category.FOOD, result.category());
        assertEquals(0.95, result.confidence());
        assertEquals("Transaction at grocery store", result.reasoning());
        assertFalse(result.humanReviewRequired());
        assertEquals("evt-1", result.eventId());
    }

    @Test
    void shouldMarkHumanReviewRequiredWhenConfidenceBelowThreshold() {
        var requestSpec = mock(ChatClient.ChatClientRequestSpec.class);
        var responseSpec = mock(ChatClient.CallResponseSpec.class);

        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.user(any(Consumer.class))).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(responseSpec);
        when(responseSpec.entity(CategoryResult.class))
            .thenReturn(new CategoryResult("ENTERTAINMENT", 0.3, "Unclear transaction pattern"));

        CategorizationResult result = service.categorize("evt-2", "WITHDRAWAL", new BigDecimal("25.00"), "USD");

        assertEquals(Category.ENTERTAINMENT, result.category());
        assertEquals(0.3, result.confidence());
        assertTrue(result.humanReviewRequired());
    }

    @Test
    void shouldFallbackToUncategorizedForUnknownCategory() {
        var requestSpec = mock(ChatClient.ChatClientRequestSpec.class);
        var responseSpec = mock(ChatClient.CallResponseSpec.class);

        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.user(any(Consumer.class))).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(responseSpec);
        when(responseSpec.entity(CategoryResult.class))
            .thenReturn(new CategoryResult("GAMBLING", 0.8, "Online casino transaction"));

        CategorizationResult result = service.categorize("evt-3", "WITHDRAWAL", new BigDecimal("100.00"), "USD");

        assertEquals(Category.UNCATEGORIZED, result.category());
        assertEquals(0.8, result.confidence());
    }

    @Test
    void shouldFallbackToUncategorizedForNullCategory() {
        var requestSpec = mock(ChatClient.ChatClientRequestSpec.class);
        var responseSpec = mock(ChatClient.CallResponseSpec.class);

        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.user(any(Consumer.class))).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(responseSpec);
        when(responseSpec.entity(CategoryResult.class))
            .thenReturn(new CategoryResult(null, 0.9, null));

        CategorizationResult result = service.categorize("evt-4", "DEPOSIT", new BigDecimal("5000.00"), "USD");

        assertEquals(Category.UNCATEGORIZED, result.category());
        assertEquals(0.9, result.confidence());
    }

    @Test
    void shouldHandleAtThresholdConfidence() {
        var requestSpec = mock(ChatClient.ChatClientRequestSpec.class);
        var responseSpec = mock(ChatClient.CallResponseSpec.class);

        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.user(any(Consumer.class))).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(responseSpec);
        when(responseSpec.entity(CategoryResult.class))
            .thenReturn(new CategoryResult("SALARY", 0.5, "Exactly at threshold"));

        CategorizationResult result = service.categorize("evt-5", "DEPOSIT", new BigDecimal("3000.00"), "USD");

        assertEquals(Category.SALARY, result.category());
        assertEquals(0.5, result.confidence());
        assertFalse(result.humanReviewRequired());
    }

    @Test
    void shouldBuildChatClientFromBuilder() {
        verify(chatClientBuilder).build();
    }
}
