package com.enterprise.batch.example.tables;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;

import java.math.BigDecimal;
import java.time.LocalDate;

public final class PaymentTable extends Table {

    public static final PaymentTable PAYMENTS = new PaymentTable("p");

    public final Column<Long>       ID;
    public final Column<Long>       ORDER_ID;
    public final Column<BigDecimal> AMOUNT;
    public final Column<String>     STATUS;
    public final Column<LocalDate>  PAYMENT_DATE;

    public PaymentTable(String alias) {
        super("payments", alias);
        this.ID           = column("id", Long.class);
        this.ORDER_ID     = column("order_id", Long.class);
        this.AMOUNT       = column("amount", BigDecimal.class);
        this.STATUS       = column("status", String.class);
        this.PAYMENT_DATE = column("payment_date", LocalDate.class);
    }

    @Override
    public PaymentTable as(String newAlias) {
        return new PaymentTable(newAlias);
    }
}
