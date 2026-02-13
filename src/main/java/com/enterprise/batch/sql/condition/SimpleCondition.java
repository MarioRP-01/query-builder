package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.ComparisonOp;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.Objects;

public class SimpleCondition implements Condition {

    private final Column<?> column;
    private final ComparisonOp op;
    private final Object value;

    public SimpleCondition(Column<?> column, ComparisonOp op, Object value) {
        Objects.requireNonNull(column, "Column must not be null");
        if (value == null) {
            throw new NullPointerException(
                "Value for " + column.name() + " is null. Use the IfPresent variant or isNull()");
        }
        this.column = column;
        this.op = op;
        this.value = value;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        String param = binder.bind(value, column.name());
        return column.ref() + " " + op.sql() + " " + param;
    }
}
