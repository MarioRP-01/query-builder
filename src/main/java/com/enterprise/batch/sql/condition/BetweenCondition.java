package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.Objects;

/**
 * {@code col [NOT] BETWEEN :p1 AND :p2}. Null-rejecting — both bounds required.
 * NOT BETWEEN via {@code negated=true}.
 */
public class BetweenCondition implements Condition {

    private final Column<?> column;
    private final Object from;
    private final Object to;
    private final boolean negated;

    public BetweenCondition(Column<?> column, Object from, Object to) {
        this(column, from, to, false);
    }

    public BetweenCondition(Column<?> column, Object from, Object to, boolean negated) {
        Objects.requireNonNull(column);
        Objects.requireNonNull(from, "BETWEEN 'from' value must not be null");
        Objects.requireNonNull(to, "BETWEEN 'to' value must not be null");
        this.column = column;
        this.from = from;
        this.to = to;
        this.negated = negated;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        String p1 = binder.bind(from, column.name() + "_from");
        String p2 = binder.bind(to, column.name() + "_to");
        return column.ref() + (negated ? " NOT BETWEEN " : " BETWEEN ") + p1 + " AND " + p2;
    }
}
