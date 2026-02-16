package com.enterprise.batch.order.application;

import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Cte;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.time.LocalDate;

import static com.enterprise.batch.order.domain.OrderTable.ORDERS;
import static com.enterprise.batch.sql.condition.Conditions.*;

/**
 * Fixed CTE — active orders (not cancelled, has date, not internal).
 * Query logic is self-contained; use via {@code .with(ACTIVE_ORDERS)}.
 */
public final class ActiveOrdersCte extends Cte {

    public static final ActiveOrdersCte ACTIVE_ORDERS = new ActiveOrdersCte("ao");

    public final Column<Long>      ID;
    public final Column<Long>      CUSTOMER_ID;
    public final Column<LocalDate> CREATED_DATE;

    public ActiveOrdersCte(String alias) {
        super("active_orders", alias);
        this.ID           = column("id", Long.class);
        this.CUSTOMER_ID  = column("customer_id", Long.class);
        this.CREATED_DATE = column("created_date", LocalDate.class);
    }

    @Override
    public SqlResult buildQuery(ParameterBinder binder) {
        return SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref(), ORDERS.CUSTOMER_ID.ref(), ORDERS.CREATED_DATE.ref())
                .from(ORDERS)
                .where(
                        neq(ORDERS.STATUS, "CANCELLED"),
                        isNotNull(ORDERS.CREATED_DATE),
                        neq(ORDERS.REGION, "INTERNAL")
                )
                .build();
    }

    @Override
    public ActiveOrdersCte as(String newAlias) {
        return new ActiveOrdersCte(newAlias);
    }
}
