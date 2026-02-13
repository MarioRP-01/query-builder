package com.enterprise.batch.sql.expression;

import com.enterprise.batch.sql.condition.Condition;
import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.List;

/**
 * Simple CASE expression: {@code CASE col WHEN val THEN val ... ELSE val END}.
 *
 * <p>Implements {@link Condition} so it can be used anywhere conditions are accepted.
 *
 * <p>Use {@link Cases#of(Column)} to build instances fluently.
 * Call {@link #as(String)} to add an alias for SELECT usage.
 */
public class SimpleCaseExpression implements Condition {

    private final Column<?> subject;
    private final List<SimpleWhenClause> whenClauses;
    private final Object elseValue;
    private final String alias;

    public SimpleCaseExpression(Column<?> subject, List<SimpleWhenClause> whenClauses,
                                Object elseValue, String alias) {
        this.subject = subject;
        this.whenClauses = List.copyOf(whenClauses);
        this.elseValue = elseValue;
        this.alias = alias;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        StringBuilder sb = new StringBuilder("CASE ").append(subject.ref());
        for (SimpleWhenClause w : whenClauses) {
            sb.append(" WHEN ").append(binder.bind(w.whenValue, subject.name()));
            sb.append(" THEN ").append(binder.bind(w.thenValue, "case"));
        }
        if (elseValue != null) {
            sb.append(" ELSE ").append(binder.bind(elseValue, "case"));
        }
        sb.append(" END");
        if (alias != null) {
            sb.append(" AS ").append(alias);
        }
        return sb.toString();
    }

    public SimpleCaseExpression as(String alias) {
        return new SimpleCaseExpression(subject, whenClauses, elseValue, alias);
    }

    public record SimpleWhenClause(Object whenValue, Object thenValue) {}
}
