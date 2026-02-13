package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.Objects;

/**
 * {@code col [NOT] LIKE :pattern}. Pattern is parameterized, never inlined.
 * NOT LIKE via {@code negated=true}.
 */
public class LikeCondition implements Condition {

    private final Column<String> column;
    private final String pattern;
    private final boolean negated;

    public LikeCondition(Column<String> column, String pattern, boolean negated) {
        Objects.requireNonNull(column);
        Objects.requireNonNull(pattern, "LIKE pattern must not be null");
        this.column = column;
        this.pattern = pattern;
        this.negated = negated;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        String param = binder.bind(pattern, column.name());
        return column.ref() + (negated ? " NOT LIKE " : " LIKE ") + param;
    }
}
