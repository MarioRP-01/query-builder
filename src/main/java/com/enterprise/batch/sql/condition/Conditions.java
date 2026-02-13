package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.ComparisonOp;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Static factory for creating {@link Condition} instances.
 * Designed to be imported statically for a clean DSL.
 *
 * <p>Unsupported: endsWith() (workaround: {@code like(col, "%" + suffix)}),
 * NOT BETWEEN, LIKE ESCAPE clause.
 *
 * <pre>{@code
 * import static com.enterprise.batch.sql.condition.Conditions.*;
 *
 * // Simple conditions
 * Condition c1 = eq(ORDERS.STATUS, "PENDING");
 * Condition c2 = gte(ORDERS.AMOUNT, new BigDecimal("100"));
 *
 * // Optional conditions (return null if value is null — safe for where(...) varargs)
 * Condition c3 = eqIfPresent(ORDERS.STATUS, params.get("status")); // may be null
 *
 * // Composable OR/AND (Gap #1 fix)
 * Condition c4 = or(
 *     eq(ORDERS.STATUS, "PENDING"),
 *     and(
 *         gte(ORDERS.AMOUNT, 1000),
 *         eq(CUSTOMERS.REGION, "EU")
 *     )
 * );
 *
 * // Optional OR — filters out null children, returns null if none remain
 * Condition c5 = orIfAny(
 *     eqIfPresent(ORDERS.STATUS, params.get("status")),
 *     eqIfPresent(ORDERS.CATEGORY, params.get("category"))
 * );
 * }</pre>
 */
public final class Conditions {

    private Conditions() {}

    // ==================== Strict conditions (null → exception) ====================

    public static <V> Condition eq(Column<V> column, V value) {
        return new SimpleCondition(column, ComparisonOp.EQ, value);
    }

    public static <V> Condition neq(Column<V> column, V value) {
        return new SimpleCondition(column, ComparisonOp.NEQ, value);
    }

    public static <V extends Comparable<? super V>> Condition gt(Column<V> column, V value) {
        return new SimpleCondition(column, ComparisonOp.GT, value);
    }

    public static <V extends Comparable<? super V>> Condition gte(Column<V> column, V value) {
        return new SimpleCondition(column, ComparisonOp.GTE, value);
    }

    public static <V extends Comparable<? super V>> Condition lt(Column<V> column, V value) {
        return new SimpleCondition(column, ComparisonOp.LT, value);
    }

    public static <V extends Comparable<? super V>> Condition lte(Column<V> column, V value) {
        return new SimpleCondition(column, ComparisonOp.LTE, value);
    }

    public static <V extends Comparable<? super V>> Condition between(Column<V> column,
                                                               V from, V to) {
        return new BetweenCondition(column, from, to);
    }

    public static Condition like(Column<String> column, String pattern) {
        return new LikeCondition(column, pattern, false);
    }

    public static Condition notLike(Column<String> column, String pattern) {
        return new LikeCondition(column, pattern, true);
    }

    public static Condition contains(Column<String> column, String value) {
        Objects.requireNonNull(value);
        return new LikeCondition(column, "%" + value + "%", false);
    }

    public static Condition startsWith(Column<String> column, String value) {
        Objects.requireNonNull(value);
        return new LikeCondition(column, value + "%", false);
    }

    public static <V> Condition in(Column<V> column, List<V> values) {
        return new InListCondition(column, values, false);
    }

    public static <V> Condition notIn(Column<V> column, List<V> values) {
        return new InListCondition(column, values, true);
    }

    public static Condition isNull(Column<?> column) {
        return new NullCondition(column, false);
    }

    public static Condition isNotNull(Column<?> column) {
        return new NullCondition(column, true);
    }

    // ==================== Optional conditions (null → return null) ====================
    // Gap #8 fix: explicit distinction between strict and optional

    /**
     * Returns null if value is null — designed for use with {@code where(Condition...)}
     * which filters out nulls.
     */
    public static <V> Condition eqIfPresent(Column<V> column, V value) {
        return value == null ? null : eq(column, value);
    }

    public static <V extends Comparable<? super V>> Condition gteIfPresent(Column<V> column,
                                                                    V value) {
        return value == null ? null : gte(column, value);
    }

    public static <V extends Comparable<? super V>> Condition lteIfPresent(Column<V> column,
                                                                    V value) {
        return value == null ? null : lte(column, value);
    }

    public static <V extends Comparable<? super V>> Condition betweenIfPresent(
            Column<V> column, V from, V to) {
        return (from == null || to == null) ? null : between(column, from, to);
    }

    public static Condition containsIfPresent(Column<String> column, String value) {
        return value == null ? null : contains(column, value);
    }

    @SuppressWarnings("unchecked")
    public static <V> Condition inIfPresent(Column<V> column, List<V> values) {
        return (values == null || values.isEmpty()) ? null : in(column, values);
    }

    public static <V> Condition neqIfPresent(Column<V> column, V value) {
        return value == null ? null : neq(column, value);
    }

    public static <V extends Comparable<? super V>> Condition gtIfPresent(Column<V> column,
                                                                    V value) {
        return value == null ? null : gt(column, value);
    }

    public static <V extends Comparable<? super V>> Condition ltIfPresent(Column<V> column,
                                                                    V value) {
        return value == null ? null : lt(column, value);
    }

    public static Condition likeIfPresent(Column<String> column, String pattern) {
        return pattern == null ? null : like(column, pattern);
    }

    public static Condition startsWithIfPresent(Column<String> column, String value) {
        return value == null ? null : startsWith(column, value);
    }

    @SuppressWarnings("unchecked")
    public static <V> Condition notInIfPresent(Column<V> column, List<V> values) {
        return (values == null || values.isEmpty()) ? null : notIn(column, values);
    }

    // ==================== Column comparisons ====================

    public static Condition eqColumn(Column<?> left, Column<?> right) {
        return new ColumnCondition(left, ComparisonOp.EQ, right);
    }

    public static Condition columnOp(Column<?> left, ComparisonOp op, Column<?> right) {
        return new ColumnCondition(left, op, right);
    }

    // ==================== Subquery conditions ====================

    public static Condition inSubquery(Column<?> column, SqlResult subquery) {
        return SubqueryCondition.in(column, subquery);
    }

    public static Condition notInSubquery(Column<?> column, SqlResult subquery) {
        return SubqueryCondition.notIn(column, subquery);
    }

    public static Condition exists(SqlResult subquery) {
        return new ExistsCondition(subquery, false);
    }

    public static Condition notExists(SqlResult subquery) {
        return new ExistsCondition(subquery, true);
    }

    public static <V extends Comparable<? super V>> Condition subquery(
            Column<V> column, ComparisonOp op, SqlResult subquery) {
        return SubqueryCondition.comparison(column, op, subquery);
    }

    // ==================== Composite conditions (AND/OR) ====================

    /**
     * Combines conditions with AND. Null conditions filtered out.
     * Throws if none remain after filtering.
     */
    public static Condition and(Condition... conditions) {
        List<Condition> nonNull = filterNulls(conditions);
        if (nonNull.isEmpty()) {
            throw new IllegalArgumentException("and() requires at least one non-null condition");
        }
        return nonNull.size() == 1 ? nonNull.get(0)
                : new CompositeCondition(CompositeCondition.Logic.AND, nonNull);
    }

    /**
     * Combines conditions with OR. Null conditions filtered out.
     * Throws if none remain after filtering.
     */
    public static Condition or(Condition... conditions) {
        List<Condition> nonNull = filterNulls(conditions);
        if (nonNull.isEmpty()) {
            throw new IllegalArgumentException("or() requires at least one non-null condition");
        }
        return nonNull.size() == 1 ? nonNull.get(0)
                : new CompositeCondition(CompositeCondition.Logic.OR, nonNull);
    }

    /**
     * Combines with AND, filtering out nulls. Returns null if no conditions remain.
     * Useful for optional composite conditions.
     */
    public static Condition andIfAny(Condition... conditions) {
        List<Condition> nonNull = filterNulls(conditions);
        if (nonNull.isEmpty()) return null;
        return nonNull.size() == 1 ? nonNull.get(0)
                : new CompositeCondition(CompositeCondition.Logic.AND, nonNull);
    }

    /**
     * Combines with OR, filtering out nulls. Returns null if no conditions remain.
     * Useful for optional composite conditions.
     */
    public static Condition orIfAny(Condition... conditions) {
        List<Condition> nonNull = filterNulls(conditions);
        if (nonNull.isEmpty()) return null;
        return nonNull.size() == 1 ? nonNull.get(0)
                : new CompositeCondition(CompositeCondition.Logic.OR, nonNull);
    }

    // ==================== Raw condition ====================

    /**
     * Raw SQL fragment for edge cases. Validated for injection safety.
     * Use "?" for parameter placeholders.
     */
    public static Condition raw(String sql, Object... values) {
        return new RawCondition(sql, values);
    }

    // ==================== Helpers ====================

    private static List<Condition> filterNulls(Condition[] conditions) {
        return Arrays.stream(conditions)
                .filter(Objects::nonNull)
                .toList();
    }
}
