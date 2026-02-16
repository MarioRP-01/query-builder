package com.enterprise.batch.order.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

public record OrderDto(
    Long id,
    BigDecimal amount,
    String status,
    LocalDate createdDate,
    String customerName
) {}
