package com.enterprise.batch.sql.builder;

import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.sql.param.ParameterBinder;
import com.enterprise.batch.sql.validation.ExpressionValidator;

import java.util.ArrayList;
import java.util.List;

public class UnionBuilder {

    private final ParameterBinder binder;
    private final List<UnionPart> parts = new ArrayList<>();
    private final List<String> orderByClauses = new ArrayList<>();

    private UnionBuilder(ParameterBinder binder) {
        this.binder = binder;
    }

    public static UnionBuilder create(ParameterBinder binder) {
        return new UnionBuilder(binder);
    }

    public UnionBuilder union(SqlResult query) {
        parts.add(new UnionPart(query.sql(), false));
        return this;
    }

    public UnionBuilder unionAll(SqlResult query) {
        parts.add(new UnionPart(query.sql(), true));
        return this;
    }

    public UnionBuilder orderByExpr(String expression, SortDirection dir) {
        ExpressionValidator.validateExpression(expression);
        orderByClauses.add(expression + " " + dir.name());
        return this;
    }

    public SqlResult build() {
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sql.append(parts.get(i).all ? " UNION ALL " : " UNION ");
            }
            sql.append(parts.get(i).sql);
        }
        if (!orderByClauses.isEmpty()) {
            sql.append(" ORDER BY ").append(String.join(", ", orderByClauses));
        }
        return new SqlResult(sql.toString(), binder.getParameters());
    }

    private record UnionPart(String sql, boolean all) {}
}
