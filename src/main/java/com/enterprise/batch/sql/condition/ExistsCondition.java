package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.sql.param.ParameterBinder;

public class ExistsCondition implements Condition {

    private final SqlResult subquery;
    private final boolean negated;

    public ExistsCondition(SqlResult subquery, boolean negated) {
        this.subquery = subquery;
        this.negated = negated;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        return (negated ? "NOT EXISTS" : "EXISTS") + " (" + subquery.sql() + ")";
    }
}
