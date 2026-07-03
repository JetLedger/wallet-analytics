package com.jetledger.analytics.application.categorization;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CategoryResult(
    @JsonProperty("category") String category,
    @JsonProperty("confidence") double confidence,
    @JsonProperty("reasoning") String reasoning
) {}
