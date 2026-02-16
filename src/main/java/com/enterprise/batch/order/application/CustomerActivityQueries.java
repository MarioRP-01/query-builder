package com.enterprise.batch.order.application;

import com.enterprise.batch.shared.querybridge.port.BatchQueryProvider;
import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.core.JoinType;
import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.sql.param.ParameterBinder;

import static com.enterprise.batch.order.application.ActiveOrdersCte.ACTIVE_ORDERS;
import static com.enterprise.batch.order.application.CustomerOrderDatesCte.CUSTOMER_DATES;
import static com.enterprise.batch.order.domain.CustomerTable.CUSTOMERS;
import static com.enterprise.batch.sql.condition.Conditions.*;

/**
 * Use case: find customers with their active-order payment dates.
 *
 * <p>Demonstrates chained CTEs with two modes:
 * <ol>
 *   <li>{@code active_orders} — <b>fixed</b> CTE, auto-builds via {@code with(ACTIVE_ORDERS)}</li>
 *   <li>{@code customer_order_dates} — <b>parameterized</b> CTE, caller supplies the excluded
 *       payment status via {@code with(CUSTOMER_DATES, CUSTOMER_DATES.buildQuery(binder, status))}</li>
 *   <li>Main query — LEFT JOINs customers to the second CTE</li>
 * </ol>
 *
 * <p>Produces:
 * <pre>{@code
 * WITH active_orders AS (
 *   SELECT o.id, o.customer_id, o.created_date FROM orders o
 *   WHERE o.status <> 'CANCELLED' AND o.created_date IS NOT NULL AND o.region <> 'INTERNAL'
 * ),
 * customer_order_dates AS (
 *   SELECT ao.customer_id, ao.created_date FROM payments p
 *   INNER JOIN active_orders ao ON p.order_id = ao.id
 *   WHERE p.status <> :status_N
 *   GROUP BY ao.customer_id, ao.created_date
 * )
 * SELECT c.id, cod.created_date FROM customers c
 * LEFT JOIN customer_order_dates cod ON c.id = cod.customer_id
 * WHERE c.tier <> 'INACTIVE'
 * ORDER BY c.id DESC
 * }</pre>
 */
public final class CustomerActivityQueries {

    private CustomerActivityQueries() {}

    public static BatchQueryProvider customerOrderDates() {
        return params -> {
            String excludedPaymentStatus = (String) params.getOrDefault(
                    "excludedPaymentStatus", "REFUNDED");

            ParameterBinder binder = new ParameterBinder();

            return SelectBuilder.subquery(binder)
                .with(ACTIVE_ORDERS)                                                    // fixed
                .with(CUSTOMER_DATES, CUSTOMER_DATES.buildQuery(binder, excludedPaymentStatus)) // parameterized
                .select(CUSTOMERS.ID.ref(), CUSTOMER_DATES.CREATED_DATE.ref())
                .from(CUSTOMERS)
                .join(JoinType.LEFT, CUSTOMER_DATES,
                        eqColumn(CUSTOMERS.ID, CUSTOMER_DATES.CUSTOMER_ID))
                .where(neq(CUSTOMERS.TIER, "INACTIVE"))
                .orderBy(CUSTOMERS.ID, SortDirection.DESC)
                .build();
        };
    }
}
