package com.enterprise.batch.sql.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for all table definitions. Each subclass represents a single DB table
 * and serves as the single source of truth for column names and types.
 *
 * <p>Tables support aliasing via {@link #as(String)} to handle self-joins and
 * subquery scenarios where the same table is referenced with different aliases.</p>
 *
 * <p>Example:
 * <pre>{@code
 * public final class OrderTable extends Table {
 *     public static final OrderTable ORDERS = new OrderTable("o");
 *
 *     public final Column<Long> ID;
 *     public final Column<BigDecimal> AMOUNT;
 *
 *     public OrderTable(String alias) {
 *         super("orders", alias);
 *         this.ID = column("id", Long.class);
 *         this.AMOUNT = column("amount", BigDecimal.class);
 *     }
 *
 *     @Override
 *     public OrderTable as(String newAlias) {
 *         return new OrderTable(newAlias);
 *     }
 * }
 * }</pre>
 */
public abstract class Table {

    private final String tableName;
    private final String alias;
    private final List<Column<?>> columns = new ArrayList<>();

    protected Table(String tableName, String alias) {
        this.tableName = tableName;
        this.alias = alias;
    }

    /**
     * Creates a typed column bound to this table instance.
     * Must be called in the constructor so that aliased copies get fresh columns.
     */
    protected <T> Column<T> column(String name, Class<T> type) {
        Column<T> col = new Column<>(this, name, type);
        columns.add(col);
        return col;
    }

    /**
     * Creates a new instance of this table with a different alias.
     * All columns in the new instance will reference the new alias.
     * Required for self-joins and reusing the same table in subqueries.
     */
    public abstract Table as(String newAlias);

    public String tableName() {
        return tableName;
    }

    public String alias() {
        return alias;
    }

    /**
     * Returns "tableName alias" for use in FROM / JOIN clauses.
     */
    public String declaration() {
        return tableName + " " + alias;
    }

    /**
     * Returns all columns defined on this table. Useful for schema validation.
     */
    public List<Column<?>> allColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public String toString() {
        return declaration();
    }
}
