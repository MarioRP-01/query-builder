package com.enterprise.batch.order.application;

import com.enterprise.batch.order.domain.AnalyzedOrderDto;
import com.enterprise.batch.order.domain.OrderWindowDto;

import java.math.BigDecimal;

/**
 * Derives trend and velocity insights from window function analytics.
 *
 * <p><b>Trend</b> — compares current amount to previous order within same customer:
 * FIRST (no prior order), UP (increased), DOWN (decreased or equal).
 *
 * <p><b>Velocity flag</b> — classifies regional spend concentration:
 * DOMINANT ({@literal >}50%), SIGNIFICANT ({@literal >}15%), MINOR (rest).
 */
public class OrderAnalyticsProcessor {

    private static final BigDecimal DOMINANT_THRESHOLD = new BigDecimal("0.50");
    private static final BigDecimal SIGNIFICANT_THRESHOLD = new BigDecimal("0.15");

    public AnalyzedOrderDto analyze(OrderWindowDto item) {
        String trend = deriveTrend(item);
        String velocityFlag = deriveVelocity(item.regionSpendPct());

        return new AnalyzedOrderDto(
            item.orderId(), item.amount(), item.createdDate(),
            item.customerName(), item.region(), item.tier(),
            item.customerOrderSeq(), item.customerRunningTotal(),
            item.prevAmount(), item.regionAmountRank(),
            item.regionSpendPct(), item.spendQuartile(),
            trend, velocityFlag);
    }

    private String deriveTrend(OrderWindowDto item) {
        if (item.customerOrderSeq() == 1L) {
            return "FIRST";
        }
        return item.amount().compareTo(item.prevAmount()) > 0 ? "UP" : "DOWN";
    }

    private String deriveVelocity(BigDecimal regionSpendPct) {
        if (regionSpendPct.compareTo(DOMINANT_THRESHOLD) > 0) {
            return "DOMINANT";
        }
        if (regionSpendPct.compareTo(SIGNIFICANT_THRESHOLD) > 0) {
            return "SIGNIFICANT";
        }
        return "MINOR";
    }
}
