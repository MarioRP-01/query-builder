package com.enterprise.batch.order.domain;

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
