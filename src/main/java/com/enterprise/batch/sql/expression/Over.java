package com.enterprise.batch.sql.expression;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.NullsOrder;
import com.enterprise.batch.sql.core.SortDirection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Static factory for building window (analytic) function expressions.
 *
 * <pre>{@code
 * // Aggregate windows
 * Over.sum(ORDERS.AMOUNT)
 *     .partitionBy(ORDERS.CUSTOMER_ID)
 *     .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
 *     .as("running_total")
 *
 * // Ranking
 * Over.rowNumber()
 *     .partitionBy(ORDERS.CUSTOMER_ID)
 *     .orderBy(ORDERS.CREATED_DATE, SortDirection.DESC)
 *     .as("rn")
 *
 * // Value functions with parameterized args
 * Over.lag(ORDERS.AMOUNT, 1, BigDecimal.ZERO)
 *     .partitionBy(ORDERS.CUSTOMER_ID)
 *     .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
 *     .as("prev_amount")
 * }</pre>
 *
 * <p>Composes with existing API via {@code selectExpr()} and {@code orderByExpr()}.
 */
public final class Over {

    private Over() {}

    // ==================== Aggregate functions ====================

    public static WindowBuilder sum(Column<?> column) {
        return new WindowBuilder(b -> "SUM(" + column.ref() + ")");
    }

    public static WindowBuilder sumDistinct(Column<?> column) {
        return new WindowBuilder(b -> "SUM(DISTINCT " + column.ref() + ")");
    }

    public static WindowBuilder count(Column<?> column) {
        return new WindowBuilder(b -> "COUNT(" + column.ref() + ")");
    }

    public static WindowBuilder countDistinct(Column<?> column) {
        return new WindowBuilder(b -> "COUNT(DISTINCT " + column.ref() + ")");
    }

    /** {@code COUNT(*) OVER (...)} */
    public static WindowBuilder countAll() {
        return new WindowBuilder(b -> "COUNT(*)");
    }

    public static WindowBuilder avg(Column<?> column) {
        return new WindowBuilder(b -> "AVG(" + column.ref() + ")");
    }

    public static WindowBuilder avgDistinct(Column<?> column) {
        return new WindowBuilder(b -> "AVG(DISTINCT " + column.ref() + ")");
    }

    public static WindowBuilder min(Column<?> column) {
        return new WindowBuilder(b -> "MIN(" + column.ref() + ")");
    }

    public static WindowBuilder max(Column<?> column) {
        return new WindowBuilder(b -> "MAX(" + column.ref() + ")");
    }

    // ==================== Ranking functions ====================

    public static WindowBuilder rowNumber() {
        return new WindowBuilder(b -> "ROW_NUMBER()");
    }

    public static WindowBuilder rank() {
        return new WindowBuilder(b -> "RANK()");
    }

    public static WindowBuilder denseRank() {
        return new WindowBuilder(b -> "DENSE_RANK()");
    }

    public static WindowBuilder ntile(int buckets) {
        return new WindowBuilder(b -> "NTILE(" + b.bind(buckets, "ntile") + ")");
    }

    // ==================== Value functions ====================

    /** {@code LAG(column) OVER (...)} — offset defaults to 1. */
    public static WindowBuilder lag(Column<?> column) {
        return new WindowBuilder(b -> "LAG(" + column.ref() + ")");
    }

    /** {@code LAG(column, offset) OVER (...)} */
    public static WindowBuilder lag(Column<?> column, int offset) {
        return new WindowBuilder(b -> "LAG(" + column.ref() + ", " + b.bind(offset, "lag") + ")");
    }

    /** {@code LAG(column, offset, default) OVER (...)} */
    public static <T> WindowBuilder lag(Column<T> column, int offset, T defaultValue) {
        return new WindowBuilder(b -> "LAG(" + column.ref() + ", "
                + b.bind(offset, "lag") + ", " + b.bind(defaultValue, "lag") + ")");
    }

    /** {@code LEAD(column) OVER (...)} — offset defaults to 1. */
    public static WindowBuilder lead(Column<?> column) {
        return new WindowBuilder(b -> "LEAD(" + column.ref() + ")");
    }

    /** {@code LEAD(column, offset) OVER (...)} */
    public static WindowBuilder lead(Column<?> column, int offset) {
        return new WindowBuilder(b -> "LEAD(" + column.ref() + ", " + b.bind(offset, "lead") + ")");
    }

    /** {@code LEAD(column, offset, default) OVER (...)} */
    public static <T> WindowBuilder lead(Column<T> column, int offset, T defaultValue) {
        return new WindowBuilder(b -> "LEAD(" + column.ref() + ", "
                + b.bind(offset, "lead") + ", " + b.bind(defaultValue, "lead") + ")");
    }

    public static WindowBuilder firstValue(Column<?> column) {
        return new WindowBuilder(b -> "FIRST_VALUE(" + column.ref() + ")");
    }

    /**
     * {@code LAST_VALUE(column) OVER (...)}.
     *
     * <p><strong>Warning:</strong> Oracle's default frame is {@code RANGE BETWEEN UNBOUNDED
     * PRECEDING AND CURRENT ROW}, which makes LAST_VALUE return the current row's value
     * instead of the partition's last. Use {@link Frame#ROWS_BETWEEN_UNBOUNDED} or
     * {@link Frame#RANGE_BETWEEN_UNBOUNDED} to get the true last value.
     */
    public static WindowBuilder lastValue(Column<?> column) {
        return new WindowBuilder(b -> "LAST_VALUE(" + column.ref() + ")");
    }

    public static WindowBuilder ratioToReport(Column<?> column) {
        return new WindowBuilder(b -> "RATIO_TO_REPORT(" + column.ref() + ")");
    }

    // ==================== Builder ====================

    public static class WindowBuilder {
        private final WindowExpression.FunctionRenderer renderer;
        private final List<String> partitionColumns = new ArrayList<>();
        private final List<WindowExpression.OrderByItem> orderByItems = new ArrayList<>();
        private Frame frame;

        WindowBuilder(WindowExpression.FunctionRenderer renderer) {
            this.renderer = renderer;
        }

        public WindowBuilder partitionBy(Column<?>... columns) {
            for (Column<?> col : columns) {
                partitionColumns.add(col.ref());
            }
            return this;
        }

        public WindowBuilder orderBy(Column<?> column, SortDirection direction) {
            orderByItems.add(new WindowExpression.OrderByItem(column.ref(), direction, null));
            return this;
        }

        public WindowBuilder orderBy(Column<?> column, SortDirection direction, NullsOrder nulls) {
            orderByItems.add(new WindowExpression.OrderByItem(column.ref(), direction, nulls));
            return this;
        }

        public WindowBuilder frame(Frame frame) {
            this.frame = frame;
            return this;
        }

        /** Terminal: returns {@link WindowExpression} with an alias (for SELECT). */
        public WindowExpression as(String alias) {
            return new WindowExpression(renderer, partitionColumns, orderByItems, frame, alias);
        }

        /** Terminal: returns {@link WindowExpression} without alias (for ORDER BY). */
        public WindowExpression build() {
            return new WindowExpression(renderer, partitionColumns, orderByItems, frame, null);
        }
    }
}
