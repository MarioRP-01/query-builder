package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@code col [NOT] IN (:p1, :p2, ...)}. Empty list rejected at construction.
 */
public class InListCondition implements Condition {

    private final Column<?> column;
    private final List<?> values;
    private final boolean negated;

    public InListCondition(Column<?> column, List<?> values, boolean negated) {
        Objects.requireNonNull(column);
        Objects.requireNonNull(values);
        if (values.isEmpty()) {
            throw new IllegalArgumentException("IN list must not be empty");
        }
        this.column = column;
        this.values = values;
        this.negated = negated;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        String params = values.stream()
                .map(v -> binder.bind(v, column.name()))
                .collect(Collectors.joining(", "));
        return column.ref() + (negated ? " NOT IN (" : " IN (") + params + ")";
    }
}
