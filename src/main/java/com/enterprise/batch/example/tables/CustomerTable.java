package com.enterprise.batch.example.tables;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;

public final class CustomerTable extends Table {

    public static final CustomerTable CUSTOMERS = new CustomerTable("c");

    public final Column<Long>   ID;
    public final Column<String> NAME;
    public final Column<String> REGION;
    public final Column<String> TIER;

    public CustomerTable(String alias) {
        super("customers", alias);
        this.ID     = column("id", Long.class);
        this.NAME   = column("name", String.class);
        this.REGION = column("region", String.class);
        this.TIER   = column("tier", String.class);
    }

    @Override
    public CustomerTable as(String newAlias) {
        return new CustomerTable(newAlias);
    }
}
