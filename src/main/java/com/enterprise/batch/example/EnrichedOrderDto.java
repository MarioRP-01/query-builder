package com.enterprise.batch.example;

import java.math.BigDecimal;
import java.time.LocalDate;

public record EnrichedOrderDto(
    Long orderId,
    String customerName,
    String customerTier,
    String productName,
    BigDecimal originalAmount,
    BigDecimal taxAmount,
    BigDecimal discountAmount,
    BigDecimal finalAmount,
    String priority,
    LocalDate processedDate
) {}
