package com.enterprise.batch.order.application;

import com.enterprise.batch.shared.querybridge.port.BatchDmlProvider;
import com.enterprise.batch.sql.builder.InsertBuilder;

import static com.enterprise.batch.order.domain.OrderSummaryTable.SUMMARIES;

public final class OrderDmlProviders {

    private OrderDmlProviders() {}

    public static BatchDmlProvider insertSummary() {
        return params -> InsertBuilder.insert()
            .into(SUMMARIES)
            .columns(
                SUMMARIES.ORDER_ID, SUMMARIES.CUSTOMER_NAME,
                SUMMARIES.CUSTOMER_TIER, SUMMARIES.PRODUCT_NAME,
                SUMMARIES.ORIGINAL_AMOUNT, SUMMARIES.TAX_AMOUNT,
                SUMMARIES.DISCOUNT_AMOUNT, SUMMARIES.FINAL_AMOUNT,
                SUMMARIES.PRIORITY, SUMMARIES.PROCESSED_DATE)
            .buildTemplate();
    }
}
