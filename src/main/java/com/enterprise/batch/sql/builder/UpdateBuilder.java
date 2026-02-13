package com.enterprise.batch.sql.builder;

import com.enterprise.batch.sql.condition.Condition;
import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Fluent builder for type-safe UPDATE statements.
 *
 * <p>Unsupported: UPDATE with JOIN (multi-table), SET col = col + expr (arithmetic),
 * SET col = CASE WHEN (conditional).
 *
 * <p>{@link #build()} requires at least one WHERE condition (safety).
 * Use {@link #buildUnconditional()} for full-table updates.
 *
 * <p>SET uses unqualified column names ({@code column.name()}).
 * WHERE reuses {@code Condition.toSql()} which produces qualified
 * references ({@code alias.col}), matching Oracle syntax:
 * {@code UPDATE orders o SET amount = :v WHERE o.status = :v}.
 *
 * <p>Example:
 * <pre>{@code
 * SqlResult r = UpdateBuilder.update()
 *     .table(ORDERS)
 *     .set(ORDERS.AMOUNT, BigDecimal.valueOf(500))
 *     .set(ORDERS.STATUS, "SHIPPED")
 *     .where(eq(ORDERS.ID, 1001L))
 *     .build();
 * }</pre>
 */
public class UpdateBuilder {

    private final ParameterBinder binder;
    private Table table;
    private final List<SetClause> setClauses = new ArrayList<>();
    private final List<Condition> conditions = new ArrayList<>();
    private final List<Column<?>> returningColumns = new ArrayList<>();

    private UpdateBuilder(ParameterBinder binder) {
        this.binder = binder;
    }

    public static UpdateBuilder update() {
        return new UpdateBuilder(new ParameterBinder());
    }

    public static UpdateBuilder update(ParameterBinder sharedBinder) {
        return new UpdateBuilder(sharedBinder);
    }

    public ParameterBinder binder() {
        return binder;
    }

    public UpdateBuilder table(Table table) {
        this.table = Objects.requireNonNull(table, "table");
        return this;
    }

    /**
     * SET column = value. Value must not be null — use {@link #setNull} instead.
     */
    public <T> UpdateBuilder set(Column<T> column, T value) {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(value, "value — use setNull() for NULL");
        setClauses.add(new ValueSetClause(column, value));
        return this;
    }

    /**
     * SET column = NULL explicitly.
     */
    public UpdateBuilder setNull(Column<?> column) {
        Objects.requireNonNull(column, "column");
        setClauses.add(new NullSetClause(column));
        return this;
    }

    /**
     * SET column = value if value is non-null; skips if null.
     */
    public <T> UpdateBuilder setIfPresent(Column<T> column, T value) {
        if (value != null) {
            setClauses.add(new ValueSetClause(column, value));
        }
        return this;
    }

    /**
     * SET column = (subquery).
     */
    public UpdateBuilder setSubquery(Column<?> column, SqlResult subquery) {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(subquery, "subquery");
        setClauses.add(new SubquerySetClause(column, subquery));
        return this;
    }

    /**
     * WHERE conditions. Nulls are silently filtered (same as SelectBuilder).
     */
    public UpdateBuilder where(Condition... conditions) {
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
    public UpdateBuilder returning(Column<?>... cols) {
        this.returningColumns.addAll(Arrays.asList(cols));
        return this;
    }

    /**
     * Builds UPDATE with mandatory WHERE. Throws if no conditions.
     */
    public SqlResult build() {
        if (conditions.isEmpty()) {
            throw new IllegalStateException(
                    "WHERE required — use buildUnconditional() for full-table updates");
        }
        return doBuild();
    }

    /**
     * Builds UPDATE without requiring WHERE (full-table update).
     */
    public SqlResult buildUnconditional() {
        return doBuild();
    }

    /**
     * Template for Spring Batch: {@code :col_name} placeholders, empty param map.
     */
    public SqlResult buildTemplate() {
        Objects.requireNonNull(table, "table required — call .table(Table)");
        if (setClauses.isEmpty()) {
            throw new IllegalStateException("No SET clauses");
        }
        String setClause = setClauses.stream()
                .map(c -> c.column().name() + " = :" + c.column().name())
                .collect(Collectors.joining(", "));
        String sql = "UPDATE " + table.declaration() + " SET " + setClause;
        return new SqlResult(sql, Map.of());
    }

    // ==================== Internal ====================

    private SqlResult doBuild() {
        Objects.requireNonNull(table, "table required — call .table(Table)");
        if (setClauses.isEmpty()) {
            throw new IllegalStateException("No SET clauses");
        }

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(table.declaration()).append(" SET ");

        String setFragment = setClauses.stream()
                .map(c -> c.toSql(binder))
                .collect(Collectors.joining(", "));
        sql.append(setFragment);

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

    // ==================== SET clause types ====================

    private sealed interface SetClause {
        Column<?> column();
        String toSql(ParameterBinder binder);
    }

    private record ValueSetClause(Column<?> column, Object value) implements SetClause {
        @Override
        public String toSql(ParameterBinder binder) {
            return column.name() + " = " + binder.bind(value, column.name());
        }
    }

    private record NullSetClause(Column<?> column) implements SetClause {
        @Override
        public String toSql(ParameterBinder binder) {
            return column.name() + " = NULL";
        }
    }

    private record SubquerySetClause(Column<?> column, SqlResult subquery) implements SetClause {
        @Override
        public String toSql(ParameterBinder binder) {
            return column.name() + " = (" + subquery.sql() + ")";
        }
    }
}
