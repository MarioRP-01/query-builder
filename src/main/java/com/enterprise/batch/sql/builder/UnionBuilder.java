package com.enterprise.batch.sql.builder;

import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.sql.param.ParameterBinder;
import com.enterprise.batch.sql.validation.ExpressionValidator;

import java.util.ArrayList;
import java.util.List;

/**
 * Combines multiple SELECT queries with UNION, UNION ALL, EXCEPT, or INTERSECT.
 * All member queries must share the same {@link ParameterBinder}.
 * Optional ORDER BY applied to the combined result.
 */
public class UnionBuilder {

    /** Set operation types for combining SELECT queries. */
    public enum SetOperator {
        UNION("UNION"),
        UNION_ALL("UNION ALL"),
        EXCEPT("EXCEPT"),
        INTERSECT("INTERSECT");

        private final String sql;

        SetOperator(String sql) { this.sql = sql; }

        public String sql() { return sql; }
    }

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
        parts.add(new UnionPart(query.sql(), SetOperator.UNION));
        return this;
    }

    public UnionBuilder unionAll(SqlResult query) {
        parts.add(new UnionPart(query.sql(), SetOperator.UNION_ALL));
        return this;
    }

    public UnionBuilder except(SqlResult query) {
        parts.add(new UnionPart(query.sql(), SetOperator.EXCEPT));
        return this;
    }

    public UnionBuilder intersect(SqlResult query) {
        parts.add(new UnionPart(query.sql(), SetOperator.INTERSECT));
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
                sql.append(" ").append(parts.get(i).op.sql()).append(" ");
            }
            sql.append(parts.get(i).sql);
        }
        if (!orderByClauses.isEmpty()) {
            sql.append(" ORDER BY ").append(String.join(", ", orderByClauses));
        }
        return new SqlResult(sql.toString(), binder.getParameters());
    }

    private record UnionPart(String sql, SetOperator op) {}
}
