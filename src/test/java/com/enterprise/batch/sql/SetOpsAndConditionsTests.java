package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.*;
import com.enterprise.batch.sql.builder.UnionBuilder.SetOperator;
import com.enterprise.batch.sql.core.NullsOrder;
import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.math.BigDecimal;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.example.tables.OrderTable.ORDERS;
import static com.enterprise.batch.example.tables.CustomerTable.CUSTOMERS;

/**
 * Tests for EXCEPT/INTERSECT, notBetween, endsWith, FOR UPDATE, NULLS FIRST/LAST.
 */
public class SetOpsAndConditionsTests {

    private int passed = 0;
    private int failed = 0;

    public static void main(String[] args) {
        SetOpsAndConditionsTests t = new SetOpsAndConditionsTests();
        t.runAll();
    }

    public void runAll() {
        System.out.println("=== Running Set Ops & Conditions Tests ===\n");

        // EXCEPT / INTERSECT
        test("EXCEPT basic", this::testExceptBasic);
        test("INTERSECT basic", this::testIntersectBasic);
        test("Mixed UNION+EXCEPT+INTERSECT", this::testMixedSetOps);
        test("EXCEPT with ORDER BY", this::testExceptWithOrderBy);
        test("INTERSECT with shared binder", this::testIntersectSharedBinder);
        test("SetOperator enum sql()", this::testSetOperatorSql);

        // notBetween
        test("notBetween basic", this::testNotBetweenBasic);
        test("notBetween parameterization", this::testNotBetweenParams);
        test("notBetweenIfPresent null", this::testNotBetweenIfPresentNull);
        test("notBetweenIfPresent non-null", this::testNotBetweenIfPresentNonNull);

        // endsWith
        test("endsWith basic", this::testEndsWithBasic);
        test("endsWithIfPresent null", this::testEndsWithIfPresentNull);
        test("endsWith parameterization", this::testEndsWithParams);

        // FOR UPDATE
        test("forUpdate basic", this::testForUpdateBasic);
        test("forUpdateSkipLocked", this::testForUpdateSkipLocked);
        test("forUpdateNoWait", this::testForUpdateNoWait);
        test("forUpdate + pagination", this::testForUpdateWithPagination);

        // NULLS FIRST / NULLS LAST
        test("NULLS FIRST basic", this::testNullsFirstBasic);
        test("NULLS LAST basic", this::testNullsLastBasic);
        test("NULLS FIRST + forUpdate", this::testNullsFirstWithForUpdate);
        test("Multiple ORDER BY mixed nulls", this::testMultipleOrderByMixedNulls);
        test("NULLS LAST orderByExpr string", this::testNullsLastOrderByExprString);
        test("NULLS FIRST orderByExpr condition", this::testNullsFirstOrderByExprCondition);

        System.out.println("\n=== Set Ops & Conditions Results: " + passed + " passed, " + failed + " failed ===");
        if (failed > 0) System.exit(1);
    }

    // ==================== EXCEPT / INTERSECT ====================

    void testExceptBasic() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS).build();
        SqlResult q2 = SelectBuilder.subquery(binder).select(CUSTOMERS.ID.ref()).from(CUSTOMERS).build();
        SqlResult r = UnionBuilder.create(binder).union(q1).except(q2).build();
        assertContains(r.sql(), "EXCEPT");
        assertNotContains(r.sql(), "UNION");
        // First part has no operator prefix
        assertTrue(r.sql().startsWith("SELECT"));
    }

    void testIntersectBasic() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS).build();
        SqlResult q2 = SelectBuilder.subquery(binder).select(CUSTOMERS.ID.ref()).from(CUSTOMERS).build();
        SqlResult r = UnionBuilder.create(binder).union(q1).intersect(q2).build();
        assertContains(r.sql(), "INTERSECT");
    }

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
        assertContains(r.sql(), "UNION");
        assertContains(r.sql(), "EXCEPT");
        assertContains(r.sql(), "INTERSECT");
    }

    void testExceptWithOrderBy() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS).build();
        SqlResult q2 = SelectBuilder.subquery(binder).select(CUSTOMERS.ID.ref()).from(CUSTOMERS).build();
        SqlResult r = UnionBuilder.create(binder)
                .union(q1).except(q2)
                .orderByExpr("1", SortDirection.ASC)
                .build();
        assertContains(r.sql(), "EXCEPT");
        assertContains(r.sql(), "ORDER BY 1 ASC");
    }

    void testIntersectSharedBinder() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "A")).build();
        SqlResult q2 = SelectBuilder.subquery(binder).select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "B")).build();
        SqlResult r = UnionBuilder.create(binder).union(q1).intersect(q2).build();
        // Both params present
        assertEquals(2, r.namedParameters().size());
    }

    void testSetOperatorSql() {
        assertEquals("UNION", SetOperator.UNION.sql());
        assertEquals("UNION ALL", SetOperator.UNION_ALL.sql());
        assertEquals("EXCEPT", SetOperator.EXCEPT.sql());
        assertEquals("INTERSECT", SetOperator.INTERSECT.sql());
    }

    // ==================== notBetween ====================

    void testNotBetweenBasic() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(notBetween(ORDERS.AMOUNT, BigDecimal.ONE, BigDecimal.TEN))
                .build();
        assertContains(r.sql(), "NOT BETWEEN");
        assertContains(r.sql(), "AND");
    }

    void testNotBetweenParams() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(notBetween(ORDERS.AMOUNT, BigDecimal.valueOf(100), BigDecimal.valueOf(999)))
                .build();
        assertContains(r.sql(), ":amount_from_1");
        assertContains(r.sql(), ":amount_to_2");
        assertEquals(2, r.namedParameters().size());
    }

    void testNotBetweenIfPresentNull() {
        assertNull(notBetweenIfPresent(ORDERS.AMOUNT, null, BigDecimal.TEN));
        assertNull(notBetweenIfPresent(ORDERS.AMOUNT, BigDecimal.ONE, null));
        assertNull(notBetweenIfPresent(ORDERS.AMOUNT, (BigDecimal) null, (BigDecimal) null));
    }

    void testNotBetweenIfPresentNonNull() {
        var cond = notBetweenIfPresent(ORDERS.AMOUNT, BigDecimal.ONE, BigDecimal.TEN);
        assertNotNull(cond);
        ParameterBinder binder = new ParameterBinder();
        String sql = cond.toSql(binder);
        assertContains(sql, "NOT BETWEEN");
    }

    // ==================== endsWith ====================

    void testEndsWithBasic() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(endsWith(ORDERS.STATUS, "ING"))
                .build();
        assertContains(r.sql(), "LIKE");
        // Pattern value should be parameterized as %ING
        Object paramVal = r.namedParameters().values().iterator().next();
        assertEquals("%ING", paramVal);
    }

    void testEndsWithIfPresentNull() {
        assertNull(endsWithIfPresent(ORDERS.STATUS, null));
    }

    void testEndsWithParams() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(endsWith(ORDERS.STATUS, "ED"))
                .build();
        assertEquals(1, r.namedParameters().size());
        assertContains(r.sql(), "LIKE :status_1");
    }

    // ==================== FOR UPDATE ====================

    void testForUpdateBasic() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .forUpdate()
                .build();
        assertTrue(r.sql().endsWith("FOR UPDATE"));
    }

    void testForUpdateSkipLocked() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .forUpdateSkipLocked()
                .build();
        assertTrue(r.sql().endsWith("FOR UPDATE SKIP LOCKED"));
    }

    void testForUpdateNoWait() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .forUpdateNoWait()
                .build();
        assertTrue(r.sql().endsWith("FOR UPDATE NOWAIT"));
    }

    void testForUpdateWithPagination() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .limit(10)
                .forUpdate()
                .build();
        assertContains(r.sql(), "FETCH FIRST");
        assertTrue(r.sql().endsWith("FOR UPDATE"));
    }

    // ==================== NULLS FIRST / NULLS LAST ====================

    void testNullsFirstBasic() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.STATUS.ref())
                .from(ORDERS)
                .orderBy(ORDERS.STATUS, SortDirection.ASC, NullsOrder.NULLS_FIRST)
                .build();
        assertContains(r.sql(), "ORDER BY o.status ASC NULLS FIRST");
    }

    void testNullsLastBasic() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.STATUS.ref())
                .from(ORDERS)
                .orderBy(ORDERS.STATUS, SortDirection.DESC, NullsOrder.NULLS_LAST)
                .build();
        assertContains(r.sql(), "ORDER BY o.status DESC NULLS LAST");
    }

    void testNullsFirstWithForUpdate() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .orderBy(ORDERS.STATUS, SortDirection.ASC, NullsOrder.NULLS_FIRST)
                .forUpdate()
                .build();
        assertContains(r.sql(), "NULLS FIRST");
        assertTrue(r.sql().endsWith("FOR UPDATE"));
    }

    void testMultipleOrderByMixedNulls() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.STATUS.ref(), ORDERS.AMOUNT.ref())
                .from(ORDERS)
                .orderBy(ORDERS.STATUS, SortDirection.ASC, NullsOrder.NULLS_FIRST)
                .orderBy(ORDERS.AMOUNT, SortDirection.DESC, NullsOrder.NULLS_LAST)
                .build();
        assertContains(r.sql(), "o.status ASC NULLS FIRST, o.amount DESC NULLS LAST");
    }

    void testNullsLastOrderByExprString() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .orderByExpr("1", SortDirection.ASC, NullsOrder.NULLS_LAST)
                .build();
        assertContains(r.sql(), "ORDER BY 1 ASC NULLS LAST");
    }

    void testNullsFirstOrderByExprCondition() {
        var caseExpr = com.enterprise.batch.sql.expression.Cases
                .when(eq(ORDERS.STATUS, "URGENT")).then(1)
                .orElse(2);
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .orderByExpr(caseExpr, SortDirection.ASC, NullsOrder.NULLS_FIRST)
                .build();
        assertContains(r.sql(), "NULLS FIRST");
        assertContains(r.sql(), "CASE WHEN");
    }

    // ==================== Helpers ====================

    private void test(String name, Runnable test) {
        try {
            test.run();
            System.out.println("  \u2713 " + name);
            passed++;
        } catch (AssertionError | Exception e) {
            System.out.println("  \u2717 " + name + " -> " + e.getMessage());
            failed++;
        }
    }

    private void assertEquals(Object expected, Object actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError("Expected " + expected + " but got " + actual);
        }
    }

    private void assertContains(String haystack, String needle) {
        if (!haystack.contains(needle)) {
            throw new AssertionError("Expected to contain '" + needle + "' in:\n  " + haystack);
        }
    }

    private void assertNotContains(String haystack, String needle) {
        if (haystack.contains(needle)) {
            throw new AssertionError("Expected NOT to contain '" + needle + "' in:\n  " + haystack);
        }
    }

    private void assertNull(Object obj) {
        if (obj != null) {
            throw new AssertionError("Expected null but got " + obj);
        }
    }

    private void assertNotNull(Object obj) {
        if (obj == null) {
            throw new AssertionError("Expected non-null");
        }
    }

    private void assertTrue(boolean condition) {
        if (!condition) {
            throw new AssertionError("Expected true");
        }
    }
}
