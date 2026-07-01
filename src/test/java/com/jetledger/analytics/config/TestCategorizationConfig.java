package com.jetledger.analytics.config;

import com.jetledger.analytics.application.categorization.CategorizationService;
import com.jetledger.contracts.CategorizationResult;
import com.jetledger.contracts.Category;
import java.math.BigDecimal;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestConfiguration
public class TestCategorizationConfig {

    @Bean
    public CategorizationService categorizationService() {
        CategorizationService mock = mock(CategorizationService.class);
        when(mock.categorize(anyString(), anyString(), any(BigDecimal.class), anyString()))
            .thenReturn(new CategorizationResult("test", Category.SALARY, 0.95, "Paycheck deposit", false));
        return mock;
    }
}
