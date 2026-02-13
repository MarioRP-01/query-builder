package com.enterprise.batch.example;

import java.math.BigDecimal;
import java.time.LocalDate;

public record OrderDetailDto(
    Long orderId,
    BigDecimal amount,
    String status,
    LocalDate createdDate,
    String customerName,
    String customerTier,
    String productName,
    String productCategory
) {}
