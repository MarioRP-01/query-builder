package com.enterprise.batch.order.domain;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;

import java.math.BigDecimal;
import java.time.LocalDate;

public final class OrderTable extends Table {

    public static final OrderTable ORDERS = new OrderTable("o");

    public final Column<Long>       ID;
    public final Column<BigDecimal> AMOUNT;
    public final Column<String>     STATUS;
    public final Column<String>     CATEGORY;
    public final Column<String>     REGION;
    public final Column<Long>       CUSTOMER_ID;
    public final Column<Long>       PRODUCT_ID;
    public final Column<LocalDate>  CREATED_DATE;

    public OrderTable(String alias) {
        super("orders", alias);
        this.ID           = column("id", Long.class);
        this.AMOUNT       = column("amount", BigDecimal.class);
        this.STATUS       = column("status", String.class);
        this.CATEGORY     = column("category", String.class);
        this.REGION       = column("region", String.class);
        this.CUSTOMER_ID  = column("customer_id", Long.class);
        this.PRODUCT_ID   = column("product_id", Long.class);
        this.CREATED_DATE = column("created_date", LocalDate.class);
    }

    @Override
    public OrderTable as(String newAlias) {
        return new OrderTable(newAlias);
    }
}
