package com.enterprise.batch.sql.builder;

import com.enterprise.batch.sql.condition.Condition;
import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Fluent builder for type-safe DELETE statements.
 *
 * <p>{@link #build()} requires at least one WHERE condition (safety).
 * Use {@link #buildUnconditional()} for full-table deletes.
 *
 * <p>Example:
 * <pre>{@code
 * SqlResult r = DeleteBuilder.delete()
 *     .from(ORDERS)
 *     .where(eq(ORDERS.STATUS, "CANCELLED"))
 *     .build();
 * }</pre>
 */
public class DeleteBuilder {

    private final ParameterBinder binder;
    private Table table;
    private final List<Condition> conditions = new ArrayList<>();
    private final List<Column<?>> returningColumns = new ArrayList<>();

    private DeleteBuilder(ParameterBinder binder) {
        this.binder = binder;
    }

    public static DeleteBuilder delete() {
        return new DeleteBuilder(new ParameterBinder());
    }

    public static DeleteBuilder delete(ParameterBinder sharedBinder) {
        return new DeleteBuilder(sharedBinder);
    }

    public ParameterBinder binder() {
        return binder;
    }

    public DeleteBuilder from(Table table) {
        this.table = Objects.requireNonNull(table, "table");
        return this;
    }

    /**
     * WHERE conditions. Nulls are silently filtered.
     */
    public DeleteBuilder where(Condition... conditions) {
        for (Condition c : conditions) {
            if (c != null) {
                this.conditions.add(c);
            }
        }
        return this;
    }

    /**
     * Oracle RETURNING col INTO :col.
     */
    public DeleteBuilder returning(Column<?>... cols) {
        this.returningColumns.addAll(Arrays.asList(cols));
        return this;
    }

    /**
     * Builds DELETE with mandatory WHERE. Throws if no conditions.
     */
    public SqlResult build() {
        if (conditions.isEmpty()) {
            throw new IllegalStateException(
                    "WHERE required — use buildUnconditional() for full-table deletes");
        }
        return doBuild();
    }

    /**
     * Builds DELETE without requiring WHERE (full-table delete).
     */
    public SqlResult buildUnconditional() {
        return doBuild();
    }

    // ==================== Internal ====================

    private SqlResult doBuild() {
        Objects.requireNonNull(table, "table required — call .from(Table)");

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(table.declaration());

        if (!conditions.isEmpty()) {
            String where = conditions.stream()
                    .map(c -> c.toSql(binder))
                    .collect(Collectors.joining(" AND "));
            sql.append(" WHERE ").append(where);
        }

        appendReturning(sql);
        return new SqlResult(sql.toString(), binder.getParameters());
    }

    private void appendReturning(StringBuilder sql) {
        if (!returningColumns.isEmpty()) {
            sql.append(" RETURNING ")
               .append(returningColumns.stream().map(Column::name).collect(Collectors.joining(", ")))
               .append(" INTO ")
               .append(returningColumns.stream().map(c -> ":" + c.name()).collect(Collectors.joining(", ")));
        }
    }
}
