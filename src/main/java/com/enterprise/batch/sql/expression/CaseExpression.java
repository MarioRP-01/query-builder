package com.enterprise.batch.sql.expression;

import com.enterprise.batch.sql.condition.Condition;
import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.List;

/**
 * Searched CASE expression: {@code CASE WHEN cond THEN val ... ELSE val END}.
 *
 * <p>Implements {@link Condition} so it can be used anywhere conditions are accepted
 * (SELECT via {@code selectExpr()}, SET via {@code setCase()}, ORDER BY).
 *
 * <p>Use {@link Cases#when(Condition)} to build instances fluently.
 * Call {@link #as(String)} to add an alias for SELECT usage.
 */
public class CaseExpression implements Condition {

    private final List<WhenClause> whenClauses;
    private final Object elseValue;
    private final Column<?> elseColumn;
    private final String alias;

    public CaseExpression(List<WhenClause> whenClauses, Object elseValue,
                          Column<?> elseColumn, String alias) {
        this.whenClauses = List.copyOf(whenClauses);
        this.elseValue = elseValue;
        this.elseColumn = elseColumn;
        this.alias = alias;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        StringBuilder sb = new StringBuilder("CASE");
        for (WhenClause w : whenClauses) {
            sb.append(" WHEN ").append(w.condition.toSql(binder));
            if (w.thenColumn != null) {
                sb.append(" THEN ").append(w.thenColumn.ref());
            } else {
                sb.append(" THEN ").append(binder.bind(w.thenValue, "case"));
            }
        }
        if (elseColumn != null) {
            sb.append(" ELSE ").append(elseColumn.ref());
        } else if (elseValue != null) {
            sb.append(" ELSE ").append(binder.bind(elseValue, "case"));
        }
        sb.append(" END");
        if (alias != null) {
            sb.append(" AS ").append(alias);
        }
        return sb.toString();
    }

    public CaseExpression as(String alias) {
        return new CaseExpression(whenClauses, elseValue, elseColumn, alias);
    }

    public record WhenClause(Condition condition, Object thenValue, Column<?> thenColumn) {}
}
