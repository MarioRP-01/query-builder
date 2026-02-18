package com.enterprise.batch.sql.expression;

import com.enterprise.batch.sql.condition.Condition;
import com.enterprise.batch.sql.core.NullsOrder;
import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.List;

/**
 * Immutable window (analytic) function expression.
 *
 * <p>Implements {@link Condition} so it composes with {@code selectExpr()} and
 * {@code orderByExpr()} without any changes to {@link com.enterprise.batch.sql.builder.SelectBuilder}.
 *
 * <p>Use {@link Over} to build instances fluently.
 */
public class WindowExpression implements Condition {

    /** Deferred renderer that can bind parameters at render time (for LAG/LEAD/NTILE args). */
    @FunctionalInterface
    interface FunctionRenderer {
        String render(ParameterBinder binder);
    }

    record OrderByItem(String columnRef, SortDirection direction, NullsOrder nullsOrder) {
        String toSql() {
            StringBuilder sb = new StringBuilder(columnRef).append(' ').append(direction.name());
            if (nullsOrder != null) {
                sb.append(' ').append(nullsOrder.sql());
            }
            return sb.toString();
        }
    }

    private final FunctionRenderer renderer;
    private final List<String> partitionColumns;
    private final List<OrderByItem> orderByItems;
    private final Frame frame;
    private final String alias;

    WindowExpression(FunctionRenderer renderer, List<String> partitionColumns,
                     List<OrderByItem> orderByItems, Frame frame, String alias) {
        this.renderer = renderer;
        this.partitionColumns = List.copyOf(partitionColumns);
        this.orderByItems = List.copyOf(orderByItems);
        this.frame = frame;
        this.alias = alias;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        StringBuilder sb = new StringBuilder(renderer.render(binder));
        sb.append(" OVER (");

        boolean hasContent = false;
        if (!partitionColumns.isEmpty()) {
            sb.append("PARTITION BY ").append(String.join(", ", partitionColumns));
            hasContent = true;
        }
        if (!orderByItems.isEmpty()) {
            if (hasContent) sb.append(' ');
            sb.append("ORDER BY ");
            for (int i = 0; i < orderByItems.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(orderByItems.get(i).toSql());
            }
            hasContent = true;
        }
        if (frame != null) {
            if (hasContent) sb.append(' ');
            sb.append(frame.sql());
        }

        sb.append(')');
        if (alias != null) {
            sb.append(" AS ").append(alias);
        }
        return sb.toString();
    }

    /** Returns a copy with the given alias. */
    public WindowExpression as(String alias) {
        return new WindowExpression(renderer, partitionColumns, orderByItems, frame, alias);
    }

}
