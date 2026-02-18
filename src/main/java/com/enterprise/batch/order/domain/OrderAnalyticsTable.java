package com.enterprise.batch.order.domain;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;

import java.math.BigDecimal;
import java.time.LocalDate;

public final class OrderAnalyticsTable extends Table {

    public static final OrderAnalyticsTable ANALYTICS = new OrderAnalyticsTable("oa");

    public final Column<Long>       ORDER_ID;
    public final Column<BigDecimal> AMOUNT;
    public final Column<LocalDate>  CREATED_DATE;
    public final Column<String>     CUSTOMER_NAME;
    public final Column<String>     REGION;
    public final Column<String>     TIER;
    public final Column<Long>       CUSTOMER_ORDER_SEQ;
    public final Column<BigDecimal> CUSTOMER_RUNNING_TOTAL;
    public final Column<BigDecimal> PREV_AMOUNT;
    public final Column<Long>       REGION_AMOUNT_RANK;
    public final Column<BigDecimal> REGION_SPEND_PCT;
    public final Column<Long>       SPEND_QUARTILE;
    public final Column<String>     TREND;
    public final Column<String>     VELOCITY_FLAG;

    public OrderAnalyticsTable(String alias) {
        super("order_analytics", alias);
        this.ORDER_ID               = column("order_id", Long.class);
        this.AMOUNT                 = column("amount", BigDecimal.class);
        this.CREATED_DATE           = column("created_date", LocalDate.class);
        this.CUSTOMER_NAME          = column("customer_name", String.class);
        this.REGION                 = column("region", String.class);
        this.TIER                   = column("tier", String.class);
        this.CUSTOMER_ORDER_SEQ     = column("customer_order_seq", Long.class);
        this.CUSTOMER_RUNNING_TOTAL = column("customer_running_total", BigDecimal.class);
        this.PREV_AMOUNT            = column("prev_amount", BigDecimal.class);
        this.REGION_AMOUNT_RANK     = column("region_amount_rank", Long.class);
        this.REGION_SPEND_PCT       = column("region_spend_pct", BigDecimal.class);
        this.SPEND_QUARTILE         = column("spend_quartile", Long.class);
        this.TREND                  = column("trend", String.class);
        this.VELOCITY_FLAG          = column("velocity_flag", String.class);
    }

    @Override
    public OrderAnalyticsTable as(String newAlias) {
        return new OrderAnalyticsTable(newAlias);
    }
}
