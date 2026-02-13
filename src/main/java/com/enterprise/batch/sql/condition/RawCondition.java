package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.param.ParameterBinder;
import com.enterprise.batch.sql.validation.ExpressionValidator;

public class RawCondition implements Condition {

    private final String sql;
    private final Object[] values;

    public RawCondition(String sql, Object[] values) {
        ExpressionValidator.validateExpression(sql.replaceAll("\\?", "X"));
        this.sql = sql;
        this.values = values;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        String result = sql;
        for (Object value : values) {
            String param = binder.bind(value, "raw");
            result = result.replaceFirst("\\?", param);
        }
        return result;
    }
}
