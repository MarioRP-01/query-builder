package com.enterprise.batch.order.domain;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;

import java.math.BigDecimal;
import java.time.LocalDate;

public final class OrderSummaryTable extends Table {

    public static final OrderSummaryTable SUMMARIES = new OrderSummaryTable("os");

    public final Column<Long>       ORDER_ID;
    public final Column<String>     CUSTOMER_NAME;
    public final Column<String>     CUSTOMER_TIER;
    public final Column<String>     PRODUCT_NAME;
    public final Column<BigDecimal> ORIGINAL_AMOUNT;
    public final Column<BigDecimal> TAX_AMOUNT;
    public final Column<BigDecimal> DISCOUNT_AMOUNT;
    public final Column<BigDecimal> FINAL_AMOUNT;
    public final Column<String>     PRIORITY;
    public final Column<LocalDate>  PROCESSED_DATE;

    public OrderSummaryTable(String alias) {
        super("order_summaries", alias);
        this.ORDER_ID        = column("order_id", Long.class);
        this.CUSTOMER_NAME   = column("customer_name", String.class);
        this.CUSTOMER_TIER   = column("customer_tier", String.class);
        this.PRODUCT_NAME    = column("product_name", String.class);
        this.ORIGINAL_AMOUNT = column("original_amount", BigDecimal.class);
        this.TAX_AMOUNT      = column("tax_amount", BigDecimal.class);
        this.DISCOUNT_AMOUNT = column("discount_amount", BigDecimal.class);
        this.FINAL_AMOUNT    = column("final_amount", BigDecimal.class);
        this.PRIORITY        = column("priority", String.class);
        this.PROCESSED_DATE  = column("processed_date", LocalDate.class);
    }

    @Override
    public OrderSummaryTable as(String newAlias) {
        return new OrderSummaryTable(newAlias);
    }
}
