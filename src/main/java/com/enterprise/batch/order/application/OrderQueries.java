package com.enterprise.batch.order.application;

import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.shared.querybridge.port.BatchQueryProvider;

import static com.enterprise.batch.order.domain.CustomerTable.CUSTOMERS;
import static com.enterprise.batch.order.domain.OrderTable.ORDERS;
import static com.enterprise.batch.order.domain.ProductTable.PRODUCTS;
import static com.enterprise.batch.sql.condition.Conditions.*;

public final class OrderQueries {

    private OrderQueries() {}

    public static BatchQueryProvider pendingOrders() {
        return params -> {
            String status = (String) params.getOrDefault("status", "PENDING");
            return SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), ORDERS.STATUS.ref(),
                        ORDERS.CREATED_DATE.ref(), CUSTOMERS.NAME.refAs("customer_name"))
                .from(ORDERS)
                .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
                .where(eq(ORDERS.STATUS, status))
                .orderBy(ORDERS.CREATED_DATE, SortDirection.DESC)
                .build();
        };
    }

    public static BatchQueryProvider highValueCustomers() {
        return params -> SelectBuilder.query()
            .select(CUSTOMERS.ID.ref(), CUSTOMERS.NAME.ref(), CUSTOMERS.TIER.ref())
            .from(CUSTOMERS)
            .where(eq(CUSTOMERS.TIER, "GOLD"))
            .build();
    }

    public static BatchQueryProvider orderDetails() {
        return params -> {
            String status = (String) params.getOrDefault("status", "PENDING");
            return SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), ORDERS.STATUS.ref(),
                        ORDERS.CREATED_DATE.ref(),
                        CUSTOMERS.NAME.refAs("customer_name"),
                        CUSTOMERS.TIER.refAs("customer_tier"),
                        PRODUCTS.NAME.refAs("product_name"),
                        PRODUCTS.CATEGORY.refAs("product_category"))
                .from(ORDERS)
                .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
                .innerJoin(PRODUCTS, ORDERS.PRODUCT_ID, PRODUCTS.ID)
                .where(eq(ORDERS.STATUS, status))
                .orderBy(ORDERS.AMOUNT, SortDirection.DESC)
                .build();
        };
    }
}
