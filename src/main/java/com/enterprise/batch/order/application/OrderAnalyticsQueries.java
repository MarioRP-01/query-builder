package com.enterprise.batch.order.application;

import com.enterprise.batch.shared.querybridge.port.BatchQueryProvider;
import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.sql.expression.Frame;
import com.enterprise.batch.sql.expression.Over;

import java.math.BigDecimal;

import static com.enterprise.batch.order.domain.CustomerTable.CUSTOMERS;
import static com.enterprise.batch.order.domain.OrderTable.ORDERS;
import static com.enterprise.batch.sql.condition.Conditions.*;

/**
 * Window-function-rich query for order analytics.
 *
 * <p>Produces per-order rows with six analytic columns:
 * <ul>
 *   <li>{@code customer_order_seq} — ROW_NUMBER within customer by date</li>
 *   <li>{@code customer_running_total} — cumulative SUM within customer</li>
 *   <li>{@code prev_amount} — LAG(amount) within customer</li>
 *   <li>{@code region_amount_rank} — DENSE_RANK within region by amount</li>
 *   <li>{@code region_spend_pct} — RATIO_TO_REPORT within region</li>
 *   <li>{@code spend_quartile} — NTILE(4) globally by amount</li>
 * </ul>
 */
public final class OrderAnalyticsQueries {

    private OrderAnalyticsQueries() {}

    public static BatchQueryProvider orderAnalytics() {
        return params -> SelectBuilder.query()
            .select(
                ORDERS.ID.refAs("order_id"),
                ORDERS.AMOUNT.ref(),
                ORDERS.CREATED_DATE.ref(),
                CUSTOMERS.NAME.refAs("customer_name"),
                CUSTOMERS.REGION.ref(),
                CUSTOMERS.TIER.ref())
            .selectExpr(
                // Sequence number within customer (chronological)
                Over.rowNumber()
                    .partitionBy(ORDERS.CUSTOMER_ID)
                    .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                    .as("customer_order_seq"),

                // Running total per customer
                Over.sum(ORDERS.AMOUNT)
                    .partitionBy(ORDERS.CUSTOMER_ID)
                    .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                    .frame(Frame.ROWS_UNBOUNDED_PRECEDING)
                    .as("customer_running_total"),

                // Previous order amount (0 if first)
                Over.lag(ORDERS.AMOUNT, 1, BigDecimal.ZERO)
                    .partitionBy(ORDERS.CUSTOMER_ID)
                    .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                    .as("prev_amount"),

                // Rank within region by amount (descending)
                Over.denseRank()
                    .partitionBy(CUSTOMERS.REGION)
                    .orderBy(ORDERS.AMOUNT, SortDirection.DESC)
                    .as("region_amount_rank"),

                // Percentage of regional spend
                Over.ratioToReport(ORDERS.AMOUNT)
                    .partitionBy(CUSTOMERS.REGION)
                    .as("region_spend_pct"),

                // Global spend quartile
                Over.ntile(4)
                    .orderBy(ORDERS.AMOUNT, SortDirection.DESC)
                    .as("spend_quartile"))
            .from(ORDERS)
            .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
            .where(neq(ORDERS.STATUS, "CANCELLED"))
            .orderBy(ORDERS.CUSTOMER_ID, SortDirection.ASC)
            .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
            .build();
    }
}
