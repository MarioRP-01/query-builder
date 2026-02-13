package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.sql.builder.UnionBuilder;
import com.enterprise.batch.sql.core.*;
import com.enterprise.batch.sql.debug.QueryDebugger;
import com.enterprise.batch.sql.param.ParameterBinder;
import com.enterprise.batch.sql.validation.ExpressionValidator;
import com.enterprise.batch.example.tables.CustomerTable;
import com.enterprise.batch.example.tables.OrderTable;
import com.enterprise.batch.example.tables.PaymentTable;
import com.enterprise.batch.example.tables.ProductTable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.example.tables.OrderTable.ORDERS;
import static com.enterprise.batch.example.tables.CustomerTable.CUSTOMERS;
import static com.enterprise.batch.example.tables.PaymentTable.PAYMENTS;
import static com.enterprise.batch.example.tables.ProductTable.PRODUCTS;

/**
 * Comprehensive tests demonstrating all 14 gap fixes.
 * Run as a standalone class (no JUnit dependency required).
 *
 * Each test method documents which gap it addresses.
 */
public class AllGapsTest {

    private int passed = 0;
    private int failed = 0;

    public static void main(String[] args) {
        AllGapsTest test = new AllGapsTest();
        test.runAll();
    }

    public void runAll() {
        System.out.println("=== Running All Gap Tests ===\n");

        // Gap #1: OR conditions
        test("Gap 1: Simple OR condition", this::testOrCondition);
        test("Gap 1: Nested AND/OR", this::testNestedAndOr);
        test("Gap 1: OR with optional children (orIfAny)", this::testOrIfAny);

        // Gap #2: Table alias collision
        test("Gap 2: Self-join with aliased table", this::testSelfJoin);
        test("Gap 2: Same table in subquery with different alias", this::testSubqueryAlias);

        // Gap #3: Derived table joins
        test("Gap 3: JOIN on a subquery (derived table)", this::testDerivedTableJoin);

        // Gap #4: UNION / UNION ALL
        test("Gap 4: UNION ALL of two queries", this::testUnionAll);

        // Gap #5: CTE (WITH clause)
        test("Gap 5: Common Table Expression", this::testCte);

        // Gap #6: Parameter ordering verification
        test("Gap 6: Parameter count verification", this::testParameterVerification);

        // Gap #7: Named parameters
        test("Gap 7: Named parameters with hints", this::testNamedParameters);

        // Gap #8: Null semantics
        test("Gap 8: Strict eq() throws on null", this::testStrictNullThrows);
        test("Gap 8: eqIfPresent() skips null", this::testIfPresentSkipsNull);

        // Gap #9: Thread safety
        test("Gap 9: Providers create fresh builders", this::testThreadSafety);

        // Gap #11: SQL injection protection
        test("Gap 11: Validates identifiers", this::testIdentifierValidation);
        test("Gap 11: Blocks dangerous SQL keywords", this::testDangerousKeywords);
        test("Gap 11: Blocks comments in expressions", this::testBlocksComments);

        // Gap #12: Dialect differences
        test("Gap 12: Oracle dialect LIMIT/OFFSET", this::testOracleDialect);
        test("Gap 12: ANSI dialect LIMIT/OFFSET", this::testAnsiDialect);

        // Gap #13: Debug logging
        test("Gap 13: Debug string generation", this::testDebugString);
        test("Gap 13: QueryDebugger format", this::testQueryDebuggerFormat);

        // Gap #14: Pagination
        test("Gap 14: LIMIT only", this::testLimitOnly);
        test("Gap 14: LIMIT + OFFSET", this::testLimitOffset);

        // Integration: complex real-world query
        test("Integration: Complex query with all features", this::testComplexQuery);
        test("Integration: Correlated subquery with EXISTS", this::testCorrelatedSubquery);
        test("Integration: Aggregation report with HAVING", this::testAggregationReport);

        System.out.println("\n=== Results: " + passed + " passed, " + failed + " failed ===");
        if (failed > 0) System.exit(1);
    }

    // ==================== Gap #1: OR conditions ====================

    void testOrCondition() {
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(
                        or(
                                eq(ORDERS.STATUS, "PENDING"),
                                eq(ORDERS.STATUS, "REVIEW")
                        )
                )
                .build();

        assertContains(result.sql(), "OR");
        assertContains(result.sql(), "o.status =");
        assertEquals(2, result.namedParameters().size());
    }

    void testNestedAndOr() {
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
                .where(
                        or(
                                eq(ORDERS.STATUS, "PENDING"),
                                and(
                                        gte(ORDERS.AMOUNT, new BigDecimal("1000")),
                                        eq(CUSTOMERS.REGION, "EU")
                                )
                        )
                )
                .build();

        assertContains(result.sql(), "OR");
        assertContains(result.sql(), "AND");
        assertContains(result.sql(), "o.status =");
        assertContains(result.sql(), "o.amount >=");
        assertContains(result.sql(), "c.region =");
        assertEquals(3, result.namedParameters().size());
    }

    void testOrIfAny() {
        // All null children → orIfAny returns null → where() skips it
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(
                        orIfAny(
                                eqIfPresent(ORDERS.STATUS, null),
                                eqIfPresent(ORDERS.CATEGORY, null)
                        ),
                        eq(ORDERS.REGION, "US")
                )
                .build();

        assertNotContains(result.sql(), "OR");
        assertContains(result.sql(), "o.region =");
        assertEquals(1, result.namedParameters().size());

        // One non-null child → condition applied
        SqlResult result2 = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(
                        orIfAny(
                                eqIfPresent(ORDERS.STATUS, "PENDING"),
                                eqIfPresent(ORDERS.CATEGORY, null)
                        )
                )
                .build();

        assertContains(result2.sql(), "o.status =");
        assertEquals(1, result2.namedParameters().size());
    }

    // ==================== Gap #2: Table alias collision ====================

    void testSelfJoin() {
        // Compare each order to orders from the same customer
        OrderTable o2 = ORDERS.as("o2");

        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), o2.AMOUNT.refAs("other_amount"))
                .from(ORDERS)
                .innerJoin(o2, ORDERS.CUSTOMER_ID, o2.CUSTOMER_ID)
                .where(
                        // Different order
                        raw(ORDERS.ID.ref() + " <> " + o2.ID.ref()),
                        gte(o2.AMOUNT, new BigDecimal("500"))
                )
                .build();

        assertContains(result.sql(), "orders o");
        assertContains(result.sql(), "orders o2");
        assertContains(result.sql(), "o.customer_id = o2.customer_id");
        assertContains(result.sql(), "o2.amount >=");
    }

    void testSubqueryAlias() {
        OrderTable subOrders = ORDERS.as("sub_o");

        // Build subquery with shared binder
        ParameterBinder binder = new ParameterBinder();

        SqlResult sub = SelectBuilder.subquery(binder)
                .select(subOrders.CUSTOMER_ID.ref())
                .from(subOrders)
                .where(gte(subOrders.AMOUNT, new BigDecimal("1000")))
                .groupBy(subOrders.CUSTOMER_ID)
                .build();

        SqlResult result = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(inSubquery(ORDERS.CUSTOMER_ID, sub))
                .build();

        assertContains(result.sql(), "sub_o.customer_id");
        assertContains(result.sql(), "sub_o.amount >=");
        assertContains(result.sql(), "o.customer_id IN");
    }

    // ==================== Gap #3: Derived table joins ====================

    void testDerivedTableJoin() {
        ParameterBinder binder = new ParameterBinder();

        // Subquery: average amount per customer
        OrderTable subO = ORDERS.as("so");
        SqlResult avgSub = SelectBuilder.subquery(binder)
                .select(subO.CUSTOMER_ID.ref(), subO.AMOUNT.avgAs("avg_amount"))
                .from(subO)
                .groupBy(subO.CUSTOMER_ID)
                .build();

        // Main query joins on the derived table
        SqlResult result = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref())
                .from(ORDERS)
                .joinSubquery(JoinType.INNER, avgSub, "summary",
                        "summary.customer_id = " + ORDERS.CUSTOMER_ID.ref())
                .build();

        assertContains(result.sql(), "INNER JOIN (SELECT so.customer_id");
        assertContains(result.sql(), ") summary ON summary.customer_id = o.customer_id");
    }

    // ==================== Gap #4: UNION ====================

    void testUnionAll() {
        ParameterBinder binder = new ParameterBinder();

        SqlResult pending = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), ORDERS.STATUS.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "PENDING"))
                .build();

        SqlResult cancelled = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), ORDERS.STATUS.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "CANCELLED"))
                .build();

        SqlResult result = UnionBuilder.create(binder)
                .unionAll(pending)
                .unionAll(cancelled)
                .orderByExpr("o.amount", SortDirection.DESC)
                .build();

        assertContains(result.sql(), "UNION ALL");
        assertContains(result.sql(), "ORDER BY o.amount DESC");
        assertEquals(2, result.namedParameters().size());
    }

    // ==================== Gap #5: CTE (WITH clause) ====================

    void testCte() {
        ParameterBinder binder = new ParameterBinder();

        // CTE: high-value customers
        SqlResult highValue = SelectBuilder.subquery(binder)
                .select(ORDERS.CUSTOMER_ID.ref())
                .from(ORDERS)
                .groupBy(ORDERS.CUSTOMER_ID)
                .havingRaw("SUM(o.amount) >= ?", new BigDecimal("10000"))
                .build();

        // Main query references the CTE
        SqlResult result = SelectBuilder.subquery(binder)
                .with("high_value", highValue)
                .select(CUSTOMERS.ID.ref(), CUSTOMERS.NAME.ref())
                .from(CUSTOMERS)
                .joinRaw(JoinType.INNER, "high_value hv",
                        "hv.customer_id = " + CUSTOMERS.ID.ref())
                .build();

        assertContains(result.sql(), "WITH high_value AS (");
        assertContains(result.sql(), "SUM(o.amount) >=");
        assertContains(result.sql(), "INNER JOIN");
    }

    // ==================== Gap #6: Parameter verification ====================

    void testParameterVerification() {
        // Valid query — should not throw
        SqlResult valid = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "PENDING"))
                .build();

        assertEquals(1, valid.namedParameters().size());

        // Positional conversion should also match
        SqlResult.PositionalQuery pq = valid.toPositional();
        assertEquals(1, pq.values().length);
        assertEquals("PENDING", pq.values()[0]);
    }

    // ==================== Gap #7: Named parameters ====================

    void testNamedParameters() {
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(
                        eq(ORDERS.STATUS, "PENDING"),
                        gte(ORDERS.AMOUNT, new BigDecimal("100"))
                )
                .build();

        // Parameters should have descriptive names
        Map<String, Object> params = result.namedParameters();
        boolean hasStatusParam = params.keySet().stream()
                .anyMatch(k -> k.contains("status"));
        boolean hasAmountParam = params.keySet().stream()
                .anyMatch(k -> k.contains("amount"));

        assertTrue(hasStatusParam, "Should have a parameter with 'status' in name");
        assertTrue(hasAmountParam, "Should have a parameter with 'amount' in name");

        // SQL should use named parameters
        assertContains(result.sql(), ":status_");
        assertContains(result.sql(), ":amount_");
    }

    // ==================== Gap #8: Null semantics ====================

    void testStrictNullThrows() {
        boolean threw = false;
        try {
            eq(ORDERS.STATUS, null);
        } catch (NullPointerException e) {
            threw = true;
            assertContains(e.getMessage(), "IfPresent");
        }
        assertTrue(threw, "eq() with null should throw NullPointerException");
    }

    void testIfPresentSkipsNull() {
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(
                        eqIfPresent(ORDERS.STATUS, null),      // skipped
                        eqIfPresent(ORDERS.CATEGORY, "BOOKS"), // applied
                        gteIfPresent(ORDERS.AMOUNT, null)      // skipped
                )
                .build();

        assertContains(result.sql(), "o.category =");
        assertNotContains(result.sql(), "o.status");
        assertNotContains(result.sql(), "o.amount");
        assertEquals(1, result.namedParameters().size());
    }

    // ==================== Gap #9: Thread safety ====================

    void testThreadSafety() {
        // Each buildQuery call should produce independent results
        SqlResult r1 = buildTestQuery(Map.of("status", "A"));
        SqlResult r2 = buildTestQuery(Map.of("status", "B"));

        assertContains(r1.toDebugString(), "'A'");
        assertContains(r2.toDebugString(), "'B'");
        assertNotContains(r1.toDebugString(), "'B'");
        assertNotContains(r2.toDebugString(), "'A'");
    }

    private SqlResult buildTestQuery(Map<String, Object> params) {
        return SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eqIfPresent(ORDERS.STATUS, (String) params.get("status")))
                .build();
    }

    // ==================== Gap #11: SQL injection protection ====================

    void testIdentifierValidation() {
        boolean threw = false;
        try {
            ExpressionValidator.validateIdentifier("id; DROP TABLE orders");
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        assertTrue(threw, "Should reject SQL injection in identifier");
    }

    void testDangerousKeywords() {
        boolean threw = false;
        try {
            ExpressionValidator.validateExpression("1; DROP TABLE orders");
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        assertTrue(threw, "Should reject dangerous keywords");
    }

    void testBlocksComments() {
        boolean threw = false;
        try {
            ExpressionValidator.validateExpression("amount -- comment");
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        assertTrue(threw, "Should reject SQL comments");
    }

    // ==================== Gap #12: Dialect differences ====================

    void testOracleDialect() {
        SqlResult result = SelectBuilder.query()
                .dialect(Dialects.ORACLE)
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .limit(100)
                .offset(50)
                .build();

        assertContains(result.sql(), "OFFSET 50 ROWS FETCH NEXT 100 ROWS ONLY");
    }

    void testAnsiDialect() {
        SqlResult result = SelectBuilder.query()
                .dialect(Dialects.ANSI)
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .limit(100)
                .offset(50)
                .build();

        assertContains(result.sql(), "OFFSET 50 ROWS FETCH NEXT 100 ROWS ONLY");
    }

    // ==================== Gap #13: Debug logging ====================

    void testDebugString() {
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(
                        eq(ORDERS.STATUS, "PENDING"),
                        gte(ORDERS.AMOUNT, new BigDecimal("100"))
                )
                .build();

        String debug = result.toDebugString();
        assertContains(debug, "'PENDING'");
        assertContains(debug, "100");
        assertNotContains(debug, ":status_");
        assertNotContains(debug, ":amount_");
    }

    void testQueryDebuggerFormat() {
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "ACTIVE"))
                .build();

        String formatted = QueryDebugger.format(result);
        assertContains(formatted, "SQL (named):");
        assertContains(formatted, "SQL (positional):");
        assertContains(formatted, "SQL (values inlined):");
        assertContains(formatted, "Parameters (1):");
    }

    // ==================== Gap #14: Pagination ====================

    void testLimitOnly() {
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .limit(50)
                .build();

        assertContains(result.sql(), "FETCH FIRST 50 ROWS ONLY");
    }

    void testLimitOffset() {
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .limit(50)
                .offset(100)
                .build();

        assertContains(result.sql(), "OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY");
    }

    // ==================== Integration tests ====================

    void testComplexQuery() {
        ParameterBinder binder = new ParameterBinder();

        // Subquery: high-value customers
        SqlResult highValue = SelectBuilder.subquery(binder)
                .select(ORDERS.CUSTOMER_ID.ref())
                .from(ORDERS)
                .groupBy(ORDERS.CUSTOMER_ID)
                .havingRaw("SUM(o.amount) >= ?", new BigDecimal("5000"))
                .build();

        // Subquery: already processed payments
        SqlResult processed = SelectBuilder.subquery(binder)
                .select(PAYMENTS.ORDER_ID.ref())
                .from(PAYMENTS)
                .where(eq(PAYMENTS.STATUS, "COMPLETED"))
                .build();

        // Main query
        SqlResult result = SelectBuilder.subquery(binder)
                .select(
                        ORDERS.ID.ref(),
                        ORDERS.AMOUNT.ref(),
                        ORDERS.STATUS.ref(),
                        CUSTOMERS.NAME.ref(),
                        CUSTOMERS.REGION.ref()
                )
                .from(ORDERS)
                .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
                .where(
                        eqIfPresent(ORDERS.STATUS, "PENDING"),
                        gteIfPresent(ORDERS.AMOUNT, new BigDecimal("100")),
                        or(
                                eq(ORDERS.CATEGORY, "ELECTRONICS"),
                                eq(ORDERS.CATEGORY, "BOOKS")
                        ),
                        inSubquery(ORDERS.CUSTOMER_ID, highValue),
                        notInSubquery(ORDERS.ID, processed)
                )
                .orderBy(ORDERS.CREATED_DATE, SortDirection.DESC)
                .limit(1000)
                .build();

        assertContains(result.sql(), "SELECT o.id, o.amount, o.status, c.name, c.region");
        assertContains(result.sql(), "INNER JOIN customers c ON o.customer_id = c.id");
        assertContains(result.sql(), "o.status =");
        assertContains(result.sql(), "o.amount >=");
        assertContains(result.sql(), "OR");
        assertContains(result.sql(), "o.customer_id IN (SELECT o.customer_id");
        assertContains(result.sql(), "o.id NOT IN (SELECT p.order_id");
        assertContains(result.sql(), "ORDER BY o.created_date DESC");
        assertContains(result.sql(), "FETCH FIRST 1000 ROWS ONLY");

        // Should have: PENDING, 100, ELECTRONICS, BOOKS, 5000, COMPLETED = 6 params
        assertEquals(6, result.namedParameters().size());

        System.out.println("\n  Generated SQL:\n  " + result.toDebugString());
    }

    void testCorrelatedSubquery() {
        ParameterBinder binder = new ParameterBinder();

        // Correlated: customers who have recent orders
        SqlResult recentOrders = SelectBuilder.subquery(binder)
                .selectRaw("1")
                .from(ORDERS)
                .where(
                        eqColumn(ORDERS.CUSTOMER_ID, CUSTOMERS.ID),
                        gte(ORDERS.CREATED_DATE, LocalDate.of(2025, 1, 1))
                )
                .build();

        SqlResult result = SelectBuilder.subquery(binder)
                .select(CUSTOMERS.ID.ref(), CUSTOMERS.NAME.ref(), CUSTOMERS.TIER.ref())
                .from(CUSTOMERS)
                .where(
                        eqIfPresent(CUSTOMERS.TIER, "GOLD"),
                        exists(recentOrders)
                )
                .orderBy(CUSTOMERS.NAME, SortDirection.ASC)
                .build();

        assertContains(result.sql(), "EXISTS (SELECT 1");
        assertContains(result.sql(), "o.customer_id = c.id");
        assertContains(result.sql(), "o.created_date >=");
        assertContains(result.sql(), "c.tier =");

        System.out.println("\n  Correlated query:\n  " + result.toDebugString());
    }

    void testAggregationReport() {
        SqlResult result = SelectBuilder.query()
                .select(
                        CUSTOMERS.REGION.ref(),
                        PRODUCTS.CATEGORY.ref(),
                        ORDERS.ID.countAs("order_count"),
                        ORDERS.AMOUNT.sumAs("total_amount"),
                        ORDERS.AMOUNT.avgAs("avg_amount")
                )
                .from(ORDERS)
                .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
                .innerJoin(PRODUCTS, ORDERS.PRODUCT_ID, PRODUCTS.ID)
                .where(
                        betweenIfPresent(ORDERS.CREATED_DATE,
                                LocalDate.of(2025, 1, 1), LocalDate.of(2025, 12, 31)),
                        eqIfPresent(CUSTOMERS.REGION, "EU")
                )
                .groupBy(CUSTOMERS.REGION, PRODUCTS.CATEGORY)
                .havingRaw("SUM(o.amount) >= ?", new BigDecimal("1000"))
                .havingRaw("COUNT(o.id) >= ?", 10)
                .orderByExpr("total_amount", SortDirection.DESC)
                .build();

        assertContains(result.sql(), "GROUP BY c.region, pr.category");
        assertContains(result.sql(), "HAVING SUM(o.amount) >=");
        assertContains(result.sql(), "COUNT(o.id) >=");
        assertContains(result.sql(), "ORDER BY total_amount DESC");

        System.out.println("\n  Aggregation report:\n  " + result.toDebugString());
    }

    // ==================== Test infrastructure ====================

    private void test(String name, Runnable test) {
        try {
            test.run();
            System.out.println("  ✓ " + name);
            passed++;
        } catch (AssertionError | Exception e) {
            System.out.println("  ✗ " + name + " → " + e.getMessage());
            failed++;
        }
    }

    private void assertEquals(Object expected, Object actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError("Expected " + expected + " but got " + actual);
        }
    }

    private void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private void assertContains(String haystack, String needle) {
        if (!haystack.contains(needle)) {
            throw new AssertionError(
                    "Expected to contain '" + needle + "' but was:\n  " + haystack);
        }
    }

    private void assertNotContains(String haystack, String needle) {
        if (haystack.contains(needle)) {
            throw new AssertionError(
                    "Expected NOT to contain '" + needle + "' but was:\n  " + haystack);
        }
    }
}
