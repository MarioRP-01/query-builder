package com.enterprise.batch.sql.builder;

import com.enterprise.batch.sql.condition.Condition;
import com.enterprise.batch.sql.core.*;
import com.enterprise.batch.sql.param.ParameterBinder;
import com.enterprise.batch.sql.validation.ExpressionValidator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The main entry point for building type-safe SQL queries.
 * Produces {@link SqlResult} with named parameters.
 *
 * <p>Unsupported: FOR UPDATE / FOR UPDATE SKIP LOCKED, NULLS FIRST/LAST in ORDER BY,
 * window functions (ROW_NUMBER, RANK, LAG, LEAD), RECURSIVE CTEs,
 * typed HAVING on aggregates (use {@link #havingRaw}).
 *
 * <p>Addresses all identified gaps:
 * <ul>
 *   <li>Gap #1: OR conditions via composable {@link Condition} objects</li>
 *   <li>Gap #2: Table aliasing via {@link Table#as(String)}</li>
 *   <li>Gap #3: Derived table joins via {@link #join(JoinType, SqlResult, String, String)}</li>
 *   <li>Gap #4: UNION via {@link UnionBuilder}</li>
 *   <li>Gap #5: CTE (WITH) via {@link #with(String, SqlResult)}</li>
 *   <li>Gap #6: Parameter verification in {@link SqlResult}</li>
 *   <li>Gap #7: Named parameters via {@link ParameterBinder}</li>
 *   <li>Gap #8: Explicit null semantics (strict vs IfPresent)</li>
 *   <li>Gap #11: Expression validation in raw string methods</li>
 *   <li>Gap #12: Dialect-specific SQL via {@link SqlDialect}</li>
 *   <li>Gap #14: Pagination via {@link #limit(int)} and {@link #offset(int)}</li>
 * </ul>
 *
 * <p>Example:
 * <pre>{@code
 * import static com.enterprise.batch.sql.condition.Conditions.*;
 * import static com.enterprise.batch.example.tables.OrderTable.ORDERS;
 * import static com.enterprise.batch.example.tables.CustomerTable.CUSTOMERS;
 *
 * SqlResult result = SelectBuilder.query()
 *     .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), CUSTOMERS.NAME.ref())
 *     .from(ORDERS)
 *     .join(JoinType.INNER, CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
 *     .where(
 *         eqIfPresent(ORDERS.STATUS, params.get("status")),
 *         gteIfPresent(ORDERS.AMOUNT, params.get("minAmount")),
 *         or(
 *             eq(ORDERS.CATEGORY, "ELECTRONICS"),
 *             eq(ORDERS.CATEGORY, "BOOKS")
 *         )
 *     )
 *     .orderBy(ORDERS.CREATED_DATE, SortDirection.DESC)
 *     .build();
 * }</pre>
 *
 * @see Condition
 * @see UnionBuilder
 */
public class SelectBuilder {

    private final ParameterBinder binder;
    private SqlDialect dialect;

    // CTE (WITH) clauses — Gap #5
    private final List<CteClause> ctes = new ArrayList<>();

    // SELECT
    private String selectClause;
    private boolean distinct;

    // FROM
    private String fromClause;

    // JOINs (including derived table joins — Gap #3)
    private final List<String> joins = new ArrayList<>();

    // WHERE — uses Condition objects for composable AND/OR (Gap #1)
    private final List<Condition> conditions = new ArrayList<>();

    // GROUP BY / HAVING
    private final List<String> groupByColumns = new ArrayList<>();
    private final List<Condition> havingConditions = new ArrayList<>();

    // ORDER BY
    private final List<String> orderByClauses = new ArrayList<>();

    // LIMIT / OFFSET — Gap #14
    private Integer limitValue;
    private Integer offsetValue;

    // Private constructor — use query() factory
    private SelectBuilder(ParameterBinder binder) {
        this.binder = binder;
        this.dialect = Dialects.ORACLE;
    }

    // ==================== Factory ====================

    /**
     * Creates a new SelectBuilder with a fresh ParameterBinder.
     */
    public static SelectBuilder query() {
        return new SelectBuilder(new ParameterBinder());
    }

    /**
     * Creates a new SelectBuilder sharing a ParameterBinder with the parent.
     * Used for subqueries and CTEs that need globally unique parameter names.
     */
    public static SelectBuilder subquery(ParameterBinder sharedBinder) {
        return new SelectBuilder(sharedBinder);
    }

    /**
     * Returns the shared binder (for subquery builders that need the same binder).
     */
    public ParameterBinder binder() {
        return binder;
    }

    // ==================== Dialect ====================

    public SelectBuilder dialect(SqlDialect dialect) {
        this.dialect = Objects.requireNonNull(dialect);
        return this;
    }

    // ==================== CTE (WITH clause) — Gap #5 ====================

    /**
     * Adds a Common Table Expression (WITH clause).
     * WARNING: CTE subquery MUST share parent binder via {@link #subquery(ParameterBinder)},
     * otherwise parameter names will collide.
     */
    public SelectBuilder with(String cteName, SqlResult cteQuery) {
        ExpressionValidator.validateIdentifier(cteName);
        ctes.add(new CteClause(cteName, cteQuery.sql()));
        return this;
    }

    // ==================== SELECT ====================

    public SelectBuilder select(String... columns) {
        this.selectClause = String.join(", ", columns);
        return this;
    }

    public SelectBuilder selectDistinct(String... columns) {
        this.distinct = true;
        this.selectClause = String.join(", ", columns);
        return this;
    }

    public SelectBuilder selectRaw(String expression) {
        ExpressionValidator.validateExpression(expression);
        this.selectClause = expression;
        return this;
    }

    // ==================== FROM ====================

    public SelectBuilder from(Table table) {
        this.fromClause = table.declaration();
        return this;
    }

    /**
     * FROM a derived table (subquery as source) — Gap #3 partial.
     */
    public SelectBuilder fromSubquery(SqlResult subquery, String alias) {
        ExpressionValidator.validateIdentifier(alias);
        this.fromClause = "(" + subquery.sql() + ") " + alias;
        return this;
    }

    // ==================== JOIN ====================

    /**
     * Type-safe join on two columns.
     */
    public SelectBuilder join(JoinType type, Table table,
                              Column<?> leftCol, Column<?> rightCol) {
        joins.add(type.sql() + " " + table.declaration()
                + " ON " + leftCol.eqColumn(rightCol));
        return this;
    }

    /**
     * Join with a multi-condition ON clause using Conditions.
     */
    public SelectBuilder join(JoinType type, Table table, Condition... onConditions) {
        String onClause = Arrays.stream(onConditions)
                .filter(Objects::nonNull)
                .map(c -> c.toSql(binder))
                .collect(Collectors.joining(" AND "));
        joins.add(type.sql() + " " + table.declaration() + " ON " + onClause);
        return this;
    }

    /**
     * Join on a derived table (subquery) — Gap #3 fix.
     * The subquery must have been built with the SAME ParameterBinder.
     */
    public SelectBuilder joinSubquery(JoinType type, SqlResult subquery,
                                      String alias, String onClause) {
        ExpressionValidator.validateIdentifier(alias);
        ExpressionValidator.validateExpression(onClause);
        joins.add(type.sql() + " (" + subquery.sql() + ") " + alias
                + " ON " + onClause);
        return this;
    }

    /**
     * Join on a named source (CTE reference, raw table name).
     * Use for CTEs or other cases where the join target is a name, not a subquery.
     */
    public SelectBuilder joinRaw(JoinType type, String tableOrCte,
                                 String onClause) {
        ExpressionValidator.validateExpression(tableOrCte);
        ExpressionValidator.validateExpression(onClause);
        joins.add(type.sql() + " " + tableOrCte + " ON " + onClause);
        return this;
    }

    // Convenience shortcuts
    public SelectBuilder innerJoin(Table t, Column<?> l, Column<?> r) {
        return join(JoinType.INNER, t, l, r);
    }
    public SelectBuilder leftJoin(Table t, Column<?> l, Column<?> r) {
        return join(JoinType.LEFT, t, l, r);
    }

    // ==================== WHERE — Gap #1 (OR), Gap #8 (null semantics) ====================

    /**
     * Adds WHERE conditions. Null conditions are silently filtered out,
     * enabling the "IfPresent" pattern from {@link com.enterprise.batch.sql.condition.Conditions}.
     *
     * <p>All non-null conditions are combined with AND.
     * Use {@code or(...)} or {@code and(...)} from Conditions for complex boolean logic.</p>
     */
    public SelectBuilder where(Condition... conditions) {
        for (Condition c : conditions) {
            if (c != null) {
                this.conditions.add(c);
            }
        }
        return this;
    }

    // ==================== GROUP BY / HAVING ====================

    public SelectBuilder groupBy(Column<?>... columns) {
        for (Column<?> col : columns) {
            groupByColumns.add(col.ref());
        }
        return this;
    }

    public SelectBuilder groupByExpr(String... expressions) {
        for (String expr : expressions) {
            ExpressionValidator.validateExpression(expr);
            groupByColumns.add(expr);
        }
        return this;
    }

    /**
     * HAVING with a column-level condition (e.g. {@code having(gt(col, val))}).
     * For aggregate expressions (COUNT/SUM), use {@link #havingRaw(String, Object)}.
     */
    public SelectBuilder having(Condition condition) {
        if (condition != null) {
            havingConditions.add(condition);
        }
        return this;
    }

    /**
     * HAVING with a raw aggregate expression.
     * Use "?" for parameter placeholders.
     */
    public SelectBuilder havingRaw(String expression, Object value) {
        ExpressionValidator.validateExpression(expression.replaceAll("\\?", "X"));
        if (value != null) {
            havingConditions.add(binder1 -> {
                String placeholder = binder1.bind(value, "having");
                return expression.replaceFirst("\\?", placeholder);
            });
        }
        return this;
    }

    // ==================== ORDER BY ====================

    public SelectBuilder orderBy(Column<?> column, SortDirection dir) {
        orderByClauses.add(column.ref() + " " + dir.name());
        return this;
    }

    public SelectBuilder orderByExpr(String expression, SortDirection dir) {
        ExpressionValidator.validateExpression(expression);
        orderByClauses.add(expression + " " + dir.name());
        return this;
    }

    // ==================== LIMIT / OFFSET — Gap #14 ====================

    public SelectBuilder limit(int count) {
        this.limitValue = count;
        return this;
    }

    public SelectBuilder offset(int skip) {
        this.offsetValue = skip;
        return this;
    }

    // ==================== BUILD ====================

    /**
     * Builds the final SQL query with named parameters.
     * Performs parameter verification (Gap #6).
     */
    public SqlResult build() {
        StringBuilder sql = new StringBuilder();

        // CTE (WITH) — Gap #5
        if (!ctes.isEmpty()) {
            sql.append("WITH ");
            sql.append(ctes.stream()
                    .map(cte -> cte.name + " AS (" + cte.sql + ")")
                    .collect(Collectors.joining(", ")));
            sql.append(" ");
        }

        // SELECT
        sql.append("SELECT ");
        if (distinct) sql.append("DISTINCT ");
        sql.append(selectClause);

        // FROM
        if (fromClause != null) {
            sql.append(" FROM ").append(fromClause);
        }

        // JOINs
        for (String join : joins) {
            sql.append(" ").append(join);
        }

        // WHERE
        if (!conditions.isEmpty()) {
            String whereClause = conditions.stream()
                    .map(c -> c.toSql(binder))
                    .collect(Collectors.joining(" AND "));
            sql.append(" WHERE ").append(whereClause);
        }

        // GROUP BY
        if (!groupByColumns.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupByColumns));
        }

        // HAVING
        if (!havingConditions.isEmpty()) {
            String havingClause = havingConditions.stream()
                    .map(c -> c.toSql(binder))
                    .collect(Collectors.joining(" AND "));
            sql.append(" HAVING ").append(havingClause);
        }

        // ORDER BY
        if (!orderByClauses.isEmpty()) {
            sql.append(" ORDER BY ").append(String.join(", ", orderByClauses));
        }

        // LIMIT / OFFSET — dialect-specific (Gap #12, #14)
        if (limitValue != null && offsetValue != null) {
            sql.append(" ").append(dialect.limitOffset(limitValue, offsetValue));
        } else if (limitValue != null) {
            sql.append(" ").append(dialect.limit(limitValue));
        } else if (offsetValue != null) {
            sql.append(" ").append(dialect.offset(offsetValue));
        }

        return new SqlResult(sql.toString(), binder.getParameters());
    }

    /**
     * Semantic alias for {@link #build()}, kept for readability in subquery/CTE/UNION contexts.
     * Returns a full SqlResult — parameters are tracked in the shared binder.
     */
    public SqlResult buildSubquery() {
        return build();
    }

    // ==================== Inner types ====================

    private record CteClause(String name, String sql) {}
}
