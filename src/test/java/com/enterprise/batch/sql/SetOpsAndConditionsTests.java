package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.*;
import com.enterprise.batch.sql.builder.UnionBuilder.SetOperator;
import com.enterprise.batch.sql.core.NullsOrder;
import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.sql.param.ParameterBinder;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.order.domain.OrderTable.ORDERS;
import static com.enterprise.batch.order.domain.CustomerTable.CUSTOMERS;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for EXCEPT/INTERSECT, notBetween, endsWith, FOR UPDATE, NULLS FIRST/LAST.
 */
public class SetOpsAndConditionsTests {

    // ==================== EXCEPT / INTERSECT ====================

    @Test
    void testExceptBasic() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS).build();
        SqlResult q2 = SelectBuilder.subquery(binder).select(CUSTOMERS.ID.ref()).from(CUSTOMERS).build();
        SqlResult r = UnionBuilder.create(binder).union(q1).except(q2).build();
        assertThat(r.sql()).contains("EXCEPT");
        assertThat(r.sql()).doesNotContain("UNION");
        // First part has no operator prefix
        assertThat(r.sql()).startsWith("SELECT");
    }

    @Test
    void testIntersectBasic() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS).build();
        SqlResult q2 = SelectBuilder.subquery(binder).select(CUSTOMERS.ID.ref()).from(CUSTOMERS).build();
        SqlResult r = UnionBuilder.create(binder).union(q1).intersect(q2).build();
        assertThat(r.sql()).contains("INTERSECT");
    }

    @Test
    void testMixedSetOps() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS).build();
        SqlResult q2 = SelectBuilder.subquery(binder).select(CUSTOMERS.ID.ref()).from(CUSTOMERS).build();
        SqlResult q3 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "ACTIVE")).build();
        SqlResult q4 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "CLOSED")).build();
        SqlResult r = UnionBuilder.create(binder)
                .union(q1).union(q2).except(q3).intersect(q4).build();
        assertThat(r.sql()).contains("UNION");
        assertThat(r.sql()).contains("EXCEPT");
        assertThat(r.sql()).contains("INTERSECT");
    }

    @Test
    void testExceptWithOrderBy() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS).build();
        SqlResult q2 = SelectBuilder.subquery(binder).select(CUSTOMERS.ID.ref()).from(CUSTOMERS).build();
        SqlResult r = UnionBuilder.create(binder)
                .union(q1).except(q2)
                .orderByExpr("1", SortDirection.ASC)
                .build();
        assertThat(r.sql()).contains("EXCEPT");
        assertThat(r.sql()).contains("ORDER BY 1 ASC");
    }

    @Test
    void testIntersectSharedBinder() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "A")).build();
        SqlResult q2 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "B")).build();
        SqlResult r = UnionBuilder.create(binder).union(q1).intersect(q2).build();
        // Both params present
        assertThat(r.namedParameters().size()).isEqualTo(2);
    }

    @Test
    void testSetOperatorSql() {
        assertThat(SetOperator.UNION.sql()).isEqualTo("UNION");
        assertThat(SetOperator.UNION_ALL.sql()).isEqualTo("UNION ALL");
        assertThat(SetOperator.EXCEPT.sql()).isEqualTo("EXCEPT");
        assertThat(SetOperator.INTERSECT.sql()).isEqualTo("INTERSECT");
    }

    // ==================== notBetween ====================

    @Test
    void testNotBetweenBasic() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(notBetween(ORDERS.AMOUNT, BigDecimal.ONE, BigDecimal.TEN))
                .build();
        assertThat(r.sql()).contains("NOT BETWEEN");
        assertThat(r.sql()).contains("AND");
    }

    @Test
    void testNotBetweenParams() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(notBetween(ORDERS.AMOUNT, BigDecimal.valueOf(100), BigDecimal.valueOf(999)))
                .build();
        assertThat(r.sql()).contains(":amount_from_1");
        assertThat(r.sql()).contains(":amount_to_2");
        assertThat(r.namedParameters().size()).isEqualTo(2);
    }

    @Test
    void testNotBetweenIfPresentNull() {
        assertThat(notBetweenIfPresent(ORDERS.AMOUNT, null, BigDecimal.TEN)).isNull();
        assertThat(notBetweenIfPresent(ORDERS.AMOUNT, BigDecimal.ONE, null)).isNull();
        assertThat(notBetweenIfPresent(ORDERS.AMOUNT, (BigDecimal) null, (BigDecimal) null)).isNull();
    }

    @Test
    void testNotBetweenIfPresentNonNull() {
        var cond = notBetweenIfPresent(ORDERS.AMOUNT, BigDecimal.ONE, BigDecimal.TEN);
        assertThat(cond).isNotNull();
        ParameterBinder binder = new ParameterBinder();
        String sql = cond.toSql(binder);
        assertThat(sql).contains("NOT BETWEEN");
    }

    // ==================== endsWith ====================

    @Test
    void testEndsWithBasic() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(endsWith(ORDERS.STATUS, "ING"))
                .build();
        assertThat(r.sql()).contains("LIKE");
        // Pattern value should be parameterized as %ING
        Object paramVal = r.namedParameters().values().iterator().next();
        assertThat(paramVal).isEqualTo("%ING");
    }

    @Test
    void testEndsWithIfPresentNull() {
        assertThat(endsWithIfPresent(ORDERS.STATUS, null)).isNull();
    }

    @Test
    void testEndsWithParams() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(endsWith(ORDERS.STATUS, "ED"))
                .build();
        assertThat(r.namedParameters().size()).isEqualTo(1);
        assertThat(r.sql()).contains("LIKE :status_1");
    }

    // ==================== FOR UPDATE ====================

    @Test
    void testForUpdateBasic() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .forUpdate()
                .build();
        assertThat(r.sql()).endsWith("FOR UPDATE");
    }

    @Test
    void testForUpdateSkipLocked() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .forUpdateSkipLocked()
                .build();
        assertThat(r.sql()).endsWith("FOR UPDATE SKIP LOCKED");
    }

    @Test
    void testForUpdateNoWait() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .forUpdateNoWait()
                .build();
        assertThat(r.sql()).endsWith("FOR UPDATE NOWAIT");
    }

    @Test
    void testForUpdateWithPagination() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .limit(10)
                .forUpdate()
                .build();
        assertThat(r.sql()).contains("FETCH FIRST");
        assertThat(r.sql()).endsWith("FOR UPDATE");
    }

    // ==================== NULLS FIRST / NULLS LAST ====================

    @Test
    void testNullsFirstBasic() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.STATUS.ref())
                .from(ORDERS)
                .orderBy(ORDERS.STATUS, SortDirection.ASC, NullsOrder.NULLS_FIRST)
                .build();
        assertThat(r.sql()).contains("ORDER BY o.status ASC NULLS FIRST");
    }

    @Test
    void testNullsLastBasic() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.STATUS.ref())
                .from(ORDERS)
                .orderBy(ORDERS.STATUS, SortDirection.DESC, NullsOrder.NULLS_LAST)
                .build();
        assertThat(r.sql()).contains("ORDER BY o.status DESC NULLS LAST");
    }

    @Test
    void testNullsFirstWithForUpdate() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .orderBy(ORDERS.STATUS, SortDirection.ASC, NullsOrder.NULLS_FIRST)
                .forUpdate()
                .build();
        assertThat(r.sql()).contains("NULLS FIRST");
        assertThat(r.sql()).endsWith("FOR UPDATE");
    }

    @Test
    void testMultipleOrderByMixedNulls() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.STATUS.ref(), ORDERS.AMOUNT.ref())
                .from(ORDERS)
                .orderBy(ORDERS.STATUS, SortDirection.ASC, NullsOrder.NULLS_FIRST)
                .orderBy(ORDERS.AMOUNT, SortDirection.DESC, NullsOrder.NULLS_LAST)
                .build();
        assertThat(r.sql()).contains("o.status ASC NULLS FIRST, o.amount DESC NULLS LAST");
    }

    @Test
    void testNullsLastOrderByExprString() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .orderByExpr("1", SortDirection.ASC, NullsOrder.NULLS_LAST)
                .build();
        assertThat(r.sql()).contains("ORDER BY 1 ASC NULLS LAST");
    }

    @Test
    void testNullsFirstOrderByExprCondition() {
        var caseExpr = com.enterprise.batch.sql.expression.Cases
                .when(eq(ORDERS.STATUS, "URGENT")).then(1)
                .orElse(2);
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .orderByExpr(caseExpr, SortDirection.ASC, NullsOrder.NULLS_FIRST)
                .build();
        assertThat(r.sql()).contains("NULLS FIRST");
        assertThat(r.sql()).contains("CASE WHEN");
    }
}
