package com.enterprise.batch.order.application;

import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Cte;
import com.enterprise.batch.sql.core.JoinType;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.time.LocalDate;

import static com.enterprise.batch.order.application.ActiveOrdersCte.ACTIVE_ORDERS;
import static com.enterprise.batch.order.domain.PaymentTable.PAYMENTS;
import static com.enterprise.batch.sql.condition.Conditions.*;

/**
 * Parameterized CTE — customer order dates filtered by a payment status to exclude.
 * Accepts the excluded payment status at call site; use via
 * {@code .with(CUSTOMER_DATES, CUSTOMER_DATES.buildQuery(binder, "REFUNDED"))}.
 */
public final class CustomerOrderDatesCte extends Cte {

    public static final CustomerOrderDatesCte CUSTOMER_DATES = new CustomerOrderDatesCte("cod");

    public final Column<Long>      CUSTOMER_ID;
    public final Column<LocalDate> CREATED_DATE;

    public CustomerOrderDatesCte(String alias) {
        super("customer_order_dates", alias);
        this.CUSTOMER_ID  = column("customer_id", Long.class);
        this.CREATED_DATE = column("created_date", LocalDate.class);
    }

    /**
     * Builds the CTE query excluding payments with the given status.
     *
     * @param binder                shared binder for parameter uniqueness
     * @param excludedPaymentStatus payment status to filter out (e.g. "REFUNDED")
     */
    public SqlResult buildQuery(ParameterBinder binder, String excludedPaymentStatus) {
        return SelectBuilder.subquery(binder)
                .select(ACTIVE_ORDERS.CUSTOMER_ID.ref(), ACTIVE_ORDERS.CREATED_DATE.ref())
                .from(PAYMENTS)
                .join(JoinType.INNER, ACTIVE_ORDERS,
                        eqColumn(PAYMENTS.ORDER_ID, ACTIVE_ORDERS.ID))
                .where(neq(PAYMENTS.STATUS, excludedPaymentStatus))
                .groupBy(ACTIVE_ORDERS.CUSTOMER_ID, ACTIVE_ORDERS.CREATED_DATE)
                .build();
    }

    @Override
    public CustomerOrderDatesCte as(String newAlias) {
        return new CustomerOrderDatesCte(newAlias);
    }
}
