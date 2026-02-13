package com.enterprise.batch.sql.expression;

import com.enterprise.batch.sql.condition.Condition;
import com.enterprise.batch.sql.core.Column;

import java.util.ArrayList;
import java.util.List;

/**
 * Static factory for building CASE expressions.
 *
 * <p>Searched CASE:
 * <pre>{@code
 * Cases.when(gt(ORDERS.AMOUNT, 1000)).then("High")
 *      .when(gt(ORDERS.AMOUNT, 100)).then("Medium")
 *      .orElse("Low")
 *      .as("tier")
 * }</pre>
 *
 * <p>Simple CASE:
 * <pre>{@code
 * Cases.of(ORDERS.STATUS)
 *      .when("A").then("Active")
 *      .when("B").then("Blocked")
 *      .orElse("Unknown")
 *      .as("label")
 * }</pre>
 */
public final class Cases {

    private Cases() {}

    /** Start a searched CASE expression: {@code CASE WHEN condition THEN ...} */
    public static SearchedCaseBuilder when(Condition condition) {
        return new SearchedCaseBuilder(condition);
    }

    /** Start a simple CASE expression: {@code CASE column WHEN value THEN ...} */
    public static <T> SimpleCaseBuilder<T> of(Column<T> subject) {
        return new SimpleCaseBuilder<>(subject);
    }

    // ==================== Searched CASE builder ====================

    public static class SearchedCaseBuilder {
        private final List<CaseExpression.WhenClause> whenClauses = new ArrayList<>();
        private Condition pendingCondition;

        SearchedCaseBuilder(Condition condition) {
            this.pendingCondition = condition;
        }

        public SearchedCaseBuilder then(Object value) {
            whenClauses.add(new CaseExpression.WhenClause(pendingCondition, value, null));
            pendingCondition = null;
            return this;
        }

        public SearchedCaseBuilder thenColumn(Column<?> column) {
            whenClauses.add(new CaseExpression.WhenClause(pendingCondition, null, column));
            pendingCondition = null;
            return this;
        }

        public SearchedCaseBuilder when(Condition condition) {
            this.pendingCondition = condition;
            return this;
        }

        public CaseExpression orElse(Object value) {
            return new CaseExpression(whenClauses, value, null, null);
        }

        public CaseExpression orElseColumn(Column<?> column) {
            return new CaseExpression(whenClauses, null, column, null);
        }

        public CaseExpression end() {
            return new CaseExpression(whenClauses, null, null, null);
        }
    }

    // ==================== Simple CASE builder ====================

    public static class SimpleCaseBuilder<T> {
        private final Column<T> subject;
        private final List<SimpleCaseExpression.SimpleWhenClause> whenClauses = new ArrayList<>();
        private T pendingWhenValue;

        SimpleCaseBuilder(Column<T> subject) {
            this.subject = subject;
        }

        public SimpleCaseBuilder<T> when(T value) {
            this.pendingWhenValue = value;
            return this;
        }

        public SimpleCaseBuilder<T> then(Object thenValue) {
            whenClauses.add(new SimpleCaseExpression.SimpleWhenClause(pendingWhenValue, thenValue));
            pendingWhenValue = null;
            return this;
        }

        public SimpleCaseExpression orElse(Object value) {
            return new SimpleCaseExpression(subject, whenClauses, value, null);
        }

        public SimpleCaseExpression end() {
            return new SimpleCaseExpression(subject, whenClauses, null, null);
        }
    }
}
