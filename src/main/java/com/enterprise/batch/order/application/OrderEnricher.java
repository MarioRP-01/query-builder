package com.enterprise.batch.order.application;

import com.enterprise.batch.order.domain.EnrichedOrderDto;
import com.enterprise.batch.order.domain.OrderDetailDto;

import java.math.BigDecimal;
import java.time.LocalDate;

public class OrderEnricher {

    public EnrichedOrderDto enrich(OrderDetailDto item) {
        BigDecimal tax = item.amount().multiply(new BigDecimal("0.10"));

        BigDecimal discountRate = switch (item.customerTier()) {
            case "GOLD"   -> new BigDecimal("0.15");
            case "SILVER" -> new BigDecimal("0.10");
            default       -> BigDecimal.ZERO;
        };
        BigDecimal discount = item.amount().multiply(discountRate);

        BigDecimal finalAmount = item.amount().add(tax).subtract(discount);

        String priority = item.amount().compareTo(new BigDecimal("1000")) >= 0
            ? "HIGH" : "NORMAL";

        return new EnrichedOrderDto(
            item.orderId(), item.customerName(), item.customerTier(),
            item.productName(), item.amount(), tax, discount, finalAmount,
            priority, LocalDate.now());
    }
}
