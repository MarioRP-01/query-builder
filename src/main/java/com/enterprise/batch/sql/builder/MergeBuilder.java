package com.enterprise.batch.sql.builder;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Fluent builder for Oracle MERGE (upsert) statements.
 *
 * <p>Unsupported: WHEN MATCHED AND condition THEN (conditional match),
 * WHEN NOT MATCHED AND condition THEN INSERT.
 *
 * <p>Produces:
 * <pre>
 * MERGE INTO target t
 * USING (SELECT :v1 AS col1 FROM DUAL) src
 * ON (t.id = src.id)
 * WHEN MATCHED THEN UPDATE SET t.col2 = src.col2
 * WHEN NOT MATCHED THEN INSERT (col1, col2) VALUES (src.col1, src.col2)
 * </pre>
 *
 * <p>Example:
 * <pre>{@code
 * SqlResult r = MergeBuilder.merge()
 *     .into(ORDERS)
 *     .usingDual(
 *         new ColumnValue<>(ORDERS.ID, 1001L),
 *         new ColumnValue<>(ORDERS.AMOUNT, BigDecimal.TEN))
 *     .on(ORDERS.ID)
 *     .whenMatchedUpdate(ORDERS.AMOUNT)
 *     .whenNotMatchedInsert(ORDERS.ID, ORDERS.AMOUNT)
 *     .build();
 * }</pre>
 */
public class MergeBuilder {

    private final ParameterBinder binder;
    private Table table;
    private String sourceAlias = "src";

    // Source: either DUAL-based or subquery
    private final List<ColumnValue<?>> dualColumns = new ArrayList<>();
    private SqlResult subquerySource;

    // ON key columns
    private final List<Column<?>> onColumns = new ArrayList<>();

    // WHEN MATCHED
    private final List<Column<?>> matchedUpdateColumns = new ArrayList<>();
    private final List<MatchedSetEntry<?>> matchedSetClauses = new ArrayList<>();
    private boolean matchedDelete;

    // WHEN NOT MATCHED
    private final List<Column<?>> notMatchedInsertColumns = new ArrayList<>();

    private MergeBuilder(ParameterBinder binder) {
        this.binder = binder;
    }

    public static MergeBuilder merge() {
        return new MergeBuilder(new ParameterBinder());
    }

    public static MergeBuilder merge(ParameterBinder sharedBinder) {
        return new MergeBuilder(sharedBinder);
    }

    public ParameterBinder binder() {
        return binder;
    }

    public MergeBuilder into(Table table) {
        this.table = Objects.requireNonNull(table, "table");
        return this;
    }

    /**
     * USING (SELECT :v AS col FROM DUAL) src — DUAL-based source row.
     */
    @SafeVarargs
    public final MergeBuilder usingDual(ColumnValue<?>... columnValues) {
        this.dualColumns.addAll(Arrays.asList(columnValues));
        return this;
    }

    /**
     * USING (subquery) alias — subquery source.
     */
    public MergeBuilder usingSubquery(SqlResult subquery, String alias) {
        this.subquerySource = Objects.requireNonNull(subquery, "subquery");
        this.sourceAlias = Objects.requireNonNull(alias, "alias");
        return this;
    }

    /**
     * ON (target.col = src.col [AND ...]) — match key columns.
     */
    public MergeBuilder on(Column<?>... keyColumns) {
        this.onColumns.addAll(Arrays.asList(keyColumns));
        return this;
    }

    /**
     * WHEN MATCHED THEN UPDATE SET target.col = src.col for each column.
     */
    public MergeBuilder whenMatchedUpdate(Column<?>... columns) {
        this.matchedUpdateColumns.addAll(Arrays.asList(columns));
        return this;
    }

    /**
     * Adds a literal value to the WHEN MATCHED UPDATE SET clause.
     */
    public <T> MergeBuilder whenMatchedSet(Column<T> column, T value) {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(value, "value");
        matchedSetClauses.add(new MatchedSetEntry<>(column, value));
        return this;
    }

    /**
     * WHEN NOT MATCHED THEN INSERT (cols) VALUES (src.cols).
     */
    public MergeBuilder whenNotMatchedInsert(Column<?>... columns) {
        this.notMatchedInsertColumns.addAll(Arrays.asList(columns));
        return this;
    }

    /**
     * WHEN MATCHED THEN DELETE (Oracle 10g+).
     */
    public MergeBuilder whenMatchedDelete() {
        this.matchedDelete = true;
        return this;
    }

    public SqlResult build() {
        validate();
        StringBuilder sql = new StringBuilder();

        // MERGE INTO target
        sql.append("MERGE INTO ").append(table.declaration());

        // USING
        if (subquerySource != null) {
            sql.append(" USING (").append(subquerySource.sql()).append(") ").append(sourceAlias);
        } else {
            sql.append(" USING (SELECT ");
            String selectList = dualColumns.stream()
                    .map(cv -> binder.bind(cv.value(), cv.column().name())
                            + " AS " + cv.column().name())
                    .collect(Collectors.joining(", "));
            sql.append(selectList).append(" FROM DUAL) ").append(sourceAlias);
        }

        // ON
        String onClause = onColumns.stream()
                .map(c -> c.ref() + " = " + sourceAlias + "." + c.name())
                .collect(Collectors.joining(" AND "));
        sql.append(" ON (").append(onClause).append(")");

        // WHEN MATCHED
        if (matchedDelete && matchedUpdateColumns.isEmpty() && matchedSetClauses.isEmpty()) {
            sql.append(" WHEN MATCHED THEN DELETE");
        } else if (!matchedUpdateColumns.isEmpty() || !matchedSetClauses.isEmpty()) {
            sql.append(" WHEN MATCHED THEN UPDATE SET ");
            List<String> sets = new ArrayList<>();
            for (Column<?> col : matchedUpdateColumns) {
                sets.add(col.ref() + " = " + sourceAlias + "." + col.name());
            }
            for (MatchedSetEntry<?> entry : matchedSetClauses) {
                sets.add(entry.column.ref() + " = " + binder.bind(entry.value, entry.column.name()));
            }
            sql.append(String.join(", ", sets));
        }

        // WHEN NOT MATCHED
        if (!notMatchedInsertColumns.isEmpty()) {
            String insertCols = notMatchedInsertColumns.stream()
                    .map(Column::name)
                    .collect(Collectors.joining(", "));
            String insertVals = notMatchedInsertColumns.stream()
                    .map(c -> sourceAlias + "." + c.name())
                    .collect(Collectors.joining(", "));
            sql.append(" WHEN NOT MATCHED THEN INSERT (")
               .append(insertCols).append(") VALUES (").append(insertVals).append(")");
        }

        Map<String, Object> allParams = new LinkedHashMap<>(binder.getParameters());
        if (subquerySource != null) {
            allParams.putAll(subquerySource.namedParameters());
        }
        return new SqlResult(sql.toString(), allParams);
    }

    private void validate() {
        // Target table
        Objects.requireNonNull(table, "target table required — call .into(Table)");
        // Source: DUAL or subquery
        if (dualColumns.isEmpty() && subquerySource == null) {
            throw new IllegalStateException("Source required — call .usingDual() or .usingSubquery()");
        }
        // Match key
        if (onColumns.isEmpty()) {
            throw new IllegalStateException("ON clause required — call .on(Column...)");
        }
        // At least one action: UPDATE, DELETE, or INSERT
        boolean hasWhen = !matchedUpdateColumns.isEmpty()
                || !matchedSetClauses.isEmpty()
                || matchedDelete
                || !notMatchedInsertColumns.isEmpty();
        if (!hasWhen) {
            throw new IllegalStateException("At least one WHEN clause required");
        }
    }

    /**
     * Type-safe column-value pair for {@link #usingDual}.
     */
    public record ColumnValue<T>(Column<T> column, T value) {
        public ColumnValue {
            Objects.requireNonNull(column, "column");
            Objects.requireNonNull(value, "value");
        }
    }

    private record MatchedSetEntry<T>(Column<T> column, T value) {}
}
