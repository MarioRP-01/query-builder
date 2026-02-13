package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.Objects;

public class BetweenCondition implements Condition {

    private final Column<?> column;
    private final Object from;
    private final Object to;

    public BetweenCondition(Column<?> column, Object from, Object to) {
        Objects.requireNonNull(column);
        Objects.requireNonNull(from, "BETWEEN 'from' value must not be null");
        Objects.requireNonNull(to, "BETWEEN 'to' value must not be null");
        this.column = column;
        this.from = from;
        this.to = to;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        String p1 = binder.bind(from, column.name() + "_from");
        String p2 = binder.bind(to, column.name() + "_to");
        return column.ref() + " BETWEEN " + p1 + " AND " + p2;
    }
}
