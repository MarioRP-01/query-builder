package com.enterprise.batch.evaluation.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

public record GroupElementDto(
    Long id,
    Long groupId,
    BigDecimal value,
    String category,
    LocalDate createdDate
) {}
