package com.enterprise.batch.evaluation.domain;

import java.math.BigDecimal;
import java.util.Map;

public record ConditionRule(
    String code,
    String operator,
    BigDecimal threshold,
    String aggregateKey
) {

    public boolean passes(BigDecimal elementValue, Map<String, BigDecimal> aggregates) {
        BigDecimal compareValue = aggregateKey != null
                ? aggregates.get(aggregateKey)
                : threshold;
        return switch (operator) {
            case "GT"  -> elementValue.compareTo(compareValue) > 0;
            case "GTE" -> elementValue.compareTo(compareValue) >= 0;
            case "LT"  -> elementValue.compareTo(compareValue) < 0;
            case "LTE" -> elementValue.compareTo(compareValue) <= 0;
            default -> throw new IllegalArgumentException("Unknown operator: " + operator);
        };
    }
}
