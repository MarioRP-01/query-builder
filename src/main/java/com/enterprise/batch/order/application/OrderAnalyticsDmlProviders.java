package com.enterprise.batch.order.application;

import com.enterprise.batch.shared.querybridge.port.BatchDmlProvider;
import com.enterprise.batch.sql.builder.InsertBuilder;

import static com.enterprise.batch.order.domain.OrderAnalyticsTable.ANALYTICS;

public final class OrderAnalyticsDmlProviders {

    private OrderAnalyticsDmlProviders() {}

    public static BatchDmlProvider insertAnalytics() {
        return params -> InsertBuilder.insert()
            .into(ANALYTICS)
            .columns(
                ANALYTICS.ORDER_ID, ANALYTICS.AMOUNT, ANALYTICS.CREATED_DATE,
                ANALYTICS.CUSTOMER_NAME, ANALYTICS.REGION, ANALYTICS.TIER,
                ANALYTICS.CUSTOMER_ORDER_SEQ, ANALYTICS.CUSTOMER_RUNNING_TOTAL,
                ANALYTICS.PREV_AMOUNT, ANALYTICS.REGION_AMOUNT_RANK,
                ANALYTICS.REGION_SPEND_PCT, ANALYTICS.SPEND_QUARTILE,
                ANALYTICS.TREND, ANALYTICS.VELOCITY_FLAG)
            .buildTemplate();
    }
}
