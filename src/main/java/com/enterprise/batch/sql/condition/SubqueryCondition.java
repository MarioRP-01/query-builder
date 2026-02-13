package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.ComparisonOp;
import com.enterprise.batch.sql.param.ParameterBinder;

public class SubqueryCondition implements Condition {

    private final Column<?> column;
    private final String operator;
    private final SqlResult subquery;

    private SubqueryCondition(Column<?> column, String operator, SqlResult subquery) {
        this.column = column;
        this.operator = operator;
        this.subquery = subquery;
    }

    public static SubqueryCondition in(Column<?> column, SqlResult subquery) {
        return new SubqueryCondition(column, "IN", subquery);
    }

    public static SubqueryCondition notIn(Column<?> column, SqlResult subquery) {
        return new SubqueryCondition(column, "NOT IN", subquery);
    }

    public static SubqueryCondition comparison(Column<?> column, ComparisonOp op,
                                                SqlResult subquery) {
        return new SubqueryCondition(column, op.sql(), subquery);
    }

    @Override
    public String toSql(ParameterBinder binder) {
        return column.ref() + " " + operator + " (" + subquery.sql() + ")";
    }
}
