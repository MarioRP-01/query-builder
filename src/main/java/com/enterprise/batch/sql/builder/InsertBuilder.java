package com.enterprise.batch.sql.builder;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;
import com.enterprise.batch.sql.param.ParameterBinder;
import com.enterprise.batch.sql.param.SqlLiteralFormatter;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Fluent builder for type-safe INSERT statements.
 *
 * <p>Two APIs:
 * <ul>
 *   <li>Columnar: {@code .into(T).columns(C...).values(V...).build()}</li>
 *   <li>Type-safe: {@code .into(T).set(C,V).set(C,V).build()}</li>
 * </ul>
 *
 * <p>Multi-row via repeated {@code .values()} calls produces Oracle
 * {@code INSERT ALL INTO ... SELECT 1 FROM DUAL}.
 *
 * <p>Example:
 * <pre>{@code
 * SqlResult r = InsertBuilder.insert()
 *     .into(ORDERS)
 *     .set(ORDERS.ID, 1001L)
 *     .set(ORDERS.AMOUNT, BigDecimal.TEN)
 *     .build();
 * }</pre>
 */
public class InsertBuilder {

    private final ParameterBinder binder;
    private Table table;
    private final List<Column<?>> columns = new ArrayList<>();
    private final List<Object[]> rows = new ArrayList<>();
    private final List<InsertSetEntry> setClauses = new ArrayList<>();
    private SqlResult selectFrom;
    private final List<Column<?>> returningColumns = new ArrayList<>();

    private InsertBuilder(ParameterBinder binder) {
        this.binder = binder;
    }

    public static InsertBuilder insert() {
        return new InsertBuilder(new ParameterBinder());
    }

    public static InsertBuilder insert(ParameterBinder sharedBinder) {
        return new InsertBuilder(sharedBinder);
    }

    public InsertBuilder into(Table table) {
        this.table = Objects.requireNonNull(table, "table");
        return this;
    }

    public InsertBuilder columns(Column<?>... cols) {
        this.columns.addAll(Arrays.asList(cols));
        return this;
    }

    /**
     * Adds a row of values. Null values are rejected — use {@link #valuesOrNull} instead.
     * Column count must match {@link #columns} count.
     */
    public InsertBuilder values(Object... values) {
        validateValueCount(values);
        for (Object v : values) {
            Objects.requireNonNull(v, "Use valuesOrNull() for nullable values");
        }
        rows.add(values.clone());
        return this;
    }

    /**
     * Adds a row of values where nulls are allowed (emitted as NULL literal).
     */
    public InsertBuilder valuesOrNull(Object... values) {
        validateValueCount(values);
        rows.add(values.clone());
        return this;
    }

    /**
     * Type-safe single-row SET API. Value must not be null.
     */
    public <T> InsertBuilder set(Column<T> column, T value) {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(value, "value");
        setClauses.add(new ValueSetEntry<>(column, value));
        return this;
    }

    /**
     * Inlines a SQL literal directly in the INSERT statement.
     * The value appears as a formatted constant, not a bind parameter.
     * Useful for constants that must stay fixed in {@link #buildTemplate()}.
     */
    public <T> InsertBuilder setLiteral(Column<T> column, T value) {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(value, "value");
        setClauses.add(new LiteralSetEntry<>(column, value));
        return this;
    }

    /**
     * INSERT INTO table (cols) SELECT ... FROM ...
     */
    public InsertBuilder insertFrom(SqlResult selectQuery) {
        this.selectFrom = Objects.requireNonNull(selectQuery, "selectQuery");
        return this;
    }

    /**
     * Oracle RETURNING col1, col2 INTO :col1, :col2.
     */
    public InsertBuilder returning(Column<?>... cols) {
        this.returningColumns.addAll(Arrays.asList(cols));
        return this;
    }

    public SqlResult build() {
        Objects.requireNonNull(table, "table required — call .into(Table)");
        if (selectFrom != null) {
            return buildInsertFrom();
        }
        if (!setClauses.isEmpty()) {
            return buildSetApi();
        }
        if (rows.isEmpty()) {
            throw new IllegalStateException("No values — call .values() or .set()");
        }
        if (rows.size() == 1) {
            return buildSingleRow();
        }
        return buildMultiRow();
    }

    /**
     * Template for Spring Batch: produces {@code :col_name} placeholders
     * (no counter suffix) with an empty parameter map.
     */
    public SqlResult buildTemplate() {
        Objects.requireNonNull(table, "table required — call .into(Table)");
        List<Column<?>> cols = resolveTemplateColumns();
        String colNames = cols.stream().map(Column::name).collect(Collectors.joining(", "));

        String placeholders;
        if (!setClauses.isEmpty()) {
            // Dispatch: ValueSetEntry → :col_name, LiteralSetEntry → formatted literal
            placeholders = setClauses.stream().map(entry -> {
                if (entry instanceof LiteralSetEntry<?> lit) {
                    return SqlLiteralFormatter.format(lit.value());
                }
                return ":" + entry.column().name();
            }).collect(Collectors.joining(", "));
        } else {
            placeholders = cols.stream().map(c -> ":" + c.name()).collect(Collectors.joining(", "));
        }

        String sql = "INSERT INTO " + table.tableName()
                + " (" + colNames + ") VALUES (" + placeholders + ")";
        return new SqlResult(sql, Map.of());
    }

    // ==================== Internal build methods ====================

    private SqlResult buildSingleRow() {
        String colNames = columns.stream().map(Column::name).collect(Collectors.joining(", "));
        Object[] values = rows.get(0);
        List<String> placeholders = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            if (values[i] == null) {
                placeholders.add("NULL");
            } else {
                placeholders.add(binder.bind(values[i], columns.get(i).name()));
            }
        }
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(table.tableName())
           .append(" (").append(colNames).append(")")
           .append(" VALUES (").append(String.join(", ", placeholders)).append(")");
        appendReturning(sql);
        return new SqlResult(sql.toString(), binder.getParameters());
    }

    private SqlResult buildMultiRow() {
        String colNames = columns.stream().map(Column::name).collect(Collectors.joining(", "));
        StringBuilder sql = new StringBuilder("INSERT ALL");
        for (Object[] row : rows) {
            List<String> placeholders = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                if (row[i] == null) {
                    placeholders.add("NULL");
                } else {
                    placeholders.add(binder.bind(row[i], columns.get(i).name()));
                }
            }
            sql.append(" INTO ").append(table.tableName())
               .append(" (").append(colNames).append(")")
               .append(" VALUES (").append(String.join(", ", placeholders)).append(")");
        }
        sql.append(" SELECT 1 FROM DUAL"); // required by Oracle INSERT ALL syntax
        return new SqlResult(sql.toString(), binder.getParameters());
    }

    private SqlResult buildSetApi() {
        List<String> colNames = new ArrayList<>();
        List<String> placeholders = new ArrayList<>();
        for (InsertSetEntry entry : setClauses) {
            colNames.add(entry.column().name());
            if (entry instanceof ValueSetEntry<?> v) {
                placeholders.add(binder.bind(v.value(), v.column().name()));
            } else if (entry instanceof LiteralSetEntry<?> lit) {
                placeholders.add(SqlLiteralFormatter.format(lit.value()));
            }
        }
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(table.tableName())
           .append(" (").append(String.join(", ", colNames)).append(")")
           .append(" VALUES (").append(String.join(", ", placeholders)).append(")");
        appendReturning(sql);
        return new SqlResult(sql.toString(), binder.getParameters());
    }

    private SqlResult buildInsertFrom() {
        if (columns.isEmpty()) {
            throw new IllegalStateException("columns required for insertFrom");
        }
        String colNames = columns.stream().map(Column::name).collect(Collectors.joining(", "));
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(table.tableName())
           .append(" (").append(colNames).append(") ")
           .append(selectFrom.sql());
        appendReturning(sql);
        Map<String, Object> allParams = new LinkedHashMap<>(binder.getParameters());
        allParams.putAll(selectFrom.namedParameters());
        return new SqlResult(sql.toString(), allParams);
    }

    // ==================== Helpers ====================

    private void validateValueCount(Object[] values) {
        if (!columns.isEmpty() && values.length != columns.size()) {
            throw new IllegalArgumentException(
                    "Value count (" + values.length + ") != column count (" + columns.size() + ")");
        }
    }

    private List<Column<?>> resolveTemplateColumns() {
        if (!columns.isEmpty()) return columns;
        if (!setClauses.isEmpty()) {
            return setClauses.stream()
                    .map(InsertSetEntry::column)
                    .collect(Collectors.toList());
        }
        throw new IllegalStateException("No columns — call .columns() or .set()");
    }

    private void appendReturning(StringBuilder sql) {
        if (!returningColumns.isEmpty()) {
            sql.append(" RETURNING ")
               .append(returningColumns.stream().map(Column::name).collect(Collectors.joining(", ")))
               .append(" INTO ")
               .append(returningColumns.stream().map(c -> ":" + c.name()).collect(Collectors.joining(", ")));
        }
    }

    private sealed interface InsertSetEntry {
        Column<?> column();
    }

    private record ValueSetEntry<T>(Column<T> column, T value) implements InsertSetEntry {}
    private record LiteralSetEntry<T>(Column<T> column, T value) implements InsertSetEntry {}
}
