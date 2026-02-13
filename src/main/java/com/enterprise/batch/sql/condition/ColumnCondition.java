package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.ComparisonOp;
import com.enterprise.batch.sql.param.ParameterBinder;

/**
 * Column-to-column comparison: {@code left.ref op right.ref}. No parameter binding.
 */
public class ColumnCondition implements Condition {

    private final Column<?> left;
    private final ComparisonOp op;
    private final Column<?> right;

    public ColumnCondition(Column<?> left, ComparisonOp op, Column<?> right) {
        this.left = left;
        this.op = op;
        this.right = right;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        return left.ref() + " " + op.sql() + " " + right.ref();
    }
}
