package com.enterprise.batch.sql.expression;

import com.enterprise.batch.sql.validation.ExpressionValidator;

/**
 * Window frame clauses for use with {@link WindowExpression}.
 *
 * <p>Use the predefined constants or {@link #of(String)} for custom frames.
 */
public final class Frame {

    private final String sql;

    private Frame(String sql) {
        this.sql = sql;
    }

    public String sql() {
        return sql;
    }

    public static final Frame ROWS_UNBOUNDED_PRECEDING =
            new Frame("ROWS UNBOUNDED PRECEDING");

    public static final Frame ROWS_BETWEEN_UNBOUNDED =
            new Frame("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING");

    public static final Frame RANGE_UNBOUNDED_PRECEDING =
            new Frame("RANGE UNBOUNDED PRECEDING");

    public static final Frame RANGE_BETWEEN_UNBOUNDED =
            new Frame("RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING");

    /** Custom frame clause, validated against SQL injection. */
    public static Frame of(String frameClause) {
        ExpressionValidator.validateExpression(frameClause);
        return new Frame(frameClause);
    }
}
