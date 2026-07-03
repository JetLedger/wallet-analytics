package com.jetledger.analytics.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "transactions")
public class Transaction {

    @Id
    private UUID id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "wallet_id", nullable = false)
    private UUID walletId;

    @Column(nullable = false, length = 50)
    private String type;

    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "balance_after", precision = 19, scale = 2)
    private BigDecimal balanceAfter;

    @Column(name = "correlation_id")
    private String correlationId;

    @Column(nullable = false)
    private Instant timestamp;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    @Column(length = 30)
    private String category;

    @Column(precision = 5, scale = 4)
    private BigDecimal confidence;

    @Column(name = "human_review_required")
    private Boolean humanReviewRequired;

    @Column(name = "categorized_at")
    private Instant categorizedAt;

    @Column(columnDefinition = "TEXT")
    private String reasoning;

    protected Transaction() {}

    public Transaction(UUID id, String eventId, UUID walletId, String type, BigDecimal amount, String currency,
                       BigDecimal balanceAfter, String correlationId, Instant timestamp) {
        this.id = id;
        this.eventId = eventId;
        this.walletId = walletId;
        this.type = type;
        this.amount = amount;
        this.currency = currency;
        this.balanceAfter = balanceAfter;
        this.correlationId = correlationId;
        this.timestamp = timestamp;
        this.processedAt = Instant.now();
    }

    public void applyCategorization(String category, BigDecimal confidence, boolean humanReviewRequired, String reasoning) {
        this.category = category;
        this.confidence = confidence;
        this.humanReviewRequired = humanReviewRequired;
        this.reasoning = reasoning;
        this.categorizedAt = Instant.now();
    }

    public boolean isCategorized() {
        return category != null;
    }

    public UUID getId() { return id; }
    public String getEventId() { return eventId; }
    public UUID getWalletId() { return walletId; }
    public String getType() { return type; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public BigDecimal getBalanceAfter() { return balanceAfter; }
    public String getCorrelationId() { return correlationId; }
    public Instant getTimestamp() { return timestamp; }
    public Instant getProcessedAt() { return processedAt; }
    public String getCategory() { return category; }
    public BigDecimal getConfidence() { return confidence; }
    public Boolean getHumanReviewRequired() { return humanReviewRequired; }
    public Instant getCategorizedAt() { return categorizedAt; }
    public String getReasoning() { return reasoning; }
}
