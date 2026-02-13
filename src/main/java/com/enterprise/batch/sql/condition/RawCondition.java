package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.param.ParameterBinder;
import com.enterprise.batch.sql.validation.ExpressionValidator;

/**
 * Escape hatch for SQL fragments not covered by typed conditions.
 * Uses {@code ?} placeholders bound via {@link ParameterBinder}.
 * Validated by {@link ExpressionValidator} to block injection.
 */
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
