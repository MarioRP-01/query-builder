package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.param.ParameterBinder;

/**
 * {@code col IS [NOT] NULL}. No parameter binding needed.
 */
public class NullCondition implements Condition {

    private final Column<?> column;
    private final boolean negated;

    public NullCondition(Column<?> column, boolean negated) {
        this.column = column;
        this.negated = negated;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        return column.ref() + (negated ? " IS NOT NULL" : " IS NULL");
    }
}
