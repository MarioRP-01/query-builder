package com.enterprise.batch.evaluation.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

public record FailedConditionDto(
    Long elementId,
    Long groupId,
    String conditionCode,
    BigDecimal elementValue,
    BigDecimal thresholdValue,
    LocalDate evaluatedDate
) {}
