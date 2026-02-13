package com.enterprise.batch.example;

import java.math.BigDecimal;
import java.time.LocalDate;

public record OrderDto(
    Long id,
    BigDecimal amount,
    String status,
    LocalDate createdDate,
    String customerName
) {}
