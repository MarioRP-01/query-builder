package com.enterprise.batch.example.tables;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;

import java.math.BigDecimal;

public final class ProductTable extends Table {

    public static final ProductTable PRODUCTS = new ProductTable("pr");

    public final Column<Long>       ID;
    public final Column<String>     NAME;
    public final Column<String>     CATEGORY;
    public final Column<BigDecimal> PRICE;

    public ProductTable(String alias) {
        super("products", alias);
        this.ID       = column("id", Long.class);
        this.NAME     = column("name", String.class);
        this.CATEGORY = column("category", String.class);
        this.PRICE    = column("price", BigDecimal.class);
    }

    @Override
    public ProductTable as(String newAlias) {
        return new ProductTable(newAlias);
    }
}
