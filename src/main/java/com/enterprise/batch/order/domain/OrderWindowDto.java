package com.enterprise.batch.order.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

public record OrderWindowDto(
    Long orderId,
    BigDecimal amount,
    LocalDate createdDate,
    String customerName,
    String region,
    String tier,
    Long customerOrderSeq,
    BigDecimal customerRunningTotal,
    BigDecimal prevAmount,
    Long regionAmountRank,
    BigDecimal regionSpendPct,
    Long spendQuartile
) {}
