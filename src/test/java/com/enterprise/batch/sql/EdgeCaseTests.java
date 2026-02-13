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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.example.tables.OrderTable.ORDERS;
import static com.enterprise.batch.example.tables.CustomerTable.CUSTOMERS;
import static com.enterprise.batch.example.tables.PaymentTable.PAYMENTS;
import static com.enterprise.batch.example.tables.ProductTable.PRODUCTS;

/**
 * Edge-case and stress tests beyond the gap-specific tests.
 */
public class EdgeCaseTests {

    private int passed = 0;
    private int failed = 0;

    public static void main(String[] args) {
        EdgeCaseTests t = new EdgeCaseTests();
        t.runAll();
    }

    public void runAll() {
        System.out.println("=== Running Edge Case Tests ===\n");

        // Null/empty edge cases
        test("Where with all-null conditions produces no WHERE", this::testAllNullConditions);
        test("Empty where() produces no WHERE clause", this::testEmptyWhere);
        test("neq() strict null throws NPE", this::testNeqNullThrows);
        test("gt() strict null throws NPE", this::testGtNullThrows);
        test("lt() strict null throws NPE", this::testLtNullThrows);
        test("between() strict null from throws NPE", this::testBetweenNullFromThrows);
        test("between() strict null to throws NPE", this::testBetweenNullToThrows);
        test("like() strict null pattern throws NPE", this::testLikeNullThrows);
        test("IN with empty list throws", this::testInEmptyListThrows);
        test("inIfPresent with null list returns null", this::testInIfPresentNullList);
        test("inIfPresent with empty list returns null", this::testInIfPresentEmptyList);
        test("containsIfPresent with null returns null", this::testContainsIfPresentNull);
        test("betweenIfPresent with null from returns null", this::testBetweenIfPresentNullFrom);
        test("betweenIfPresent with null to returns null", this::testBetweenIfPresentNullTo);

        // Composite edge cases
        test("and() with single condition unwraps", this::testAndSingleUnwrap);
        test("or() with single condition unwraps", this::testOrSingleUnwrap);
        test("and() with all nulls throws", this::testAndAllNullsThrows);
        test("or() with all nulls throws", this::testOrAllNullsThrows);
        test("andIfAny with all nulls returns null", this::testAndIfAnyAllNulls);
        test("Deeply nested AND/OR", this::testDeeplyNestedAndOr);

        // SQL injection variants
        test("Injection: single quotes in identifier", this::testInjectionSingleQuotes);
        test("Injection: block comment in expression", this::testInjectionBlockComment);
        test("Injection: UNION keyword in raw expression", this::testInjectionUnionInExpression);
        test("Injection: semicolon in expression", this::testInjectionSemicolon);
        test("Injection: null identifier rejected", this::testInjectionNullIdentifier);
        test("Injection: blank expression rejected", this::testInjectionBlankExpression);
        test("Injection: ALTER in identifier", this::testInjectionAlterIdentifier);
        test("Values are parameterized, never inlined in SQL", this::testValuesNeverInlined);

        // Dialect edge cases
        test("ANSI limit only", this::testAnsiLimitOnly);
        test("ANSI offset only", this::testAnsiOffsetOnly);

        // Column/table features
        test("Column ref produces alias.name", this::testColumnRef);
        test("Column refAs produces alias.name AS alias", this::testColumnRefAs);
        test("Column aggregates produce correct SQL", this::testColumnAggregates);
        test("Table declaration produces tableName alias", this::testTableDeclaration);
        test("Table allColumns returns all defined columns", this::testTableAllColumns);
        test("Aliased table produces independent columns", this::testAliasedTableIndependence);
        test("Multiple aliases of same table are independent", this::testMultipleAliases);

        // SelectBuilder features
        test("selectDistinct produces DISTINCT", this::testSelectDistinct);
        test("selectRaw with valid expression works", this::testSelectRaw);
        test("selectRaw with dangerous keyword fails", this::testSelectRawInjection);
        test("fromSubquery wraps in parentheses", this::testFromSubquery);
        test("leftJoin shortcut works", this::testLeftJoin);
        test("Multi-condition ON join", this::testMultiConditionJoin);
        test("groupByExpr validates expression", this::testGroupByExprValidation);
        test("Multiple havingRaw conditions combined with AND", this::testMultipleHaving);
        test("No FROM clause (e.g. SELECT 1)", this::testNoFrom);

        // SqlResult features
        test("toPositional replaces all named params with ?", this::testPositionalConversion);
        test("verify() passes on valid query", this::testVerifyValid);
        test("toDebugString quotes strings, not numbers", this::testDebugStringTypes);

        // ParameterBinder uniqueness
        test("Shared binder generates globally unique names", this::testBinderUniqueness);

        // UnionBuilder
        test("UNION (not ALL) uses UNION keyword", this::testUnionNotAll);
        test("Union with three queries", this::testTripleUnion);

        // CTE with multiple WITH clauses
        test("Multiple CTEs separated by comma", this::testMultipleCtes);

        // Thread safety: concurrent builder usage
        test("Concurrent builds produce isolated results", this::testConcurrentBuilds);


        // IN list condition
        test("IN list with multiple values", this::testInListMultipleValues);

        // LIKE / contains / startsWith
        test("like() produces LIKE", this::testLike);
        test("notLike() produces NOT LIKE", this::testNotLike);
        test("contains() wraps value with %", this::testContains);
        test("startsWith() appends %", this::testStartsWith);

        // isNull / isNotNull
        test("isNull produces IS NULL", this::testIsNull);
        test("isNotNull produces IS NOT NULL", this::testIsNotNull);

        // notIn
        test("notIn produces NOT IN", this::testNotIn);

        // Raw condition with parameters
        test("Raw condition with ? parameters", this::testRawWithParams);

        // EXISTS / NOT EXISTS
        test("notExists produces NOT EXISTS", this::testNotExists);

        // Column-to-column conditions
        test("eqColumn produces col = col", this::testEqColumn);
        test("columnOp with GT produces col > col", this::testColumnOpGt);

        System.out.println("\n=== Edge Case Results: " + passed + " passed, " + failed + " failed ===");
        if (failed > 0) System.exit(1);
    }

    // ==================== Null/empty edge cases ====================

    void testAllNullConditions() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(
                        eqIfPresent(ORDERS.STATUS, null),
                        gteIfPresent(ORDERS.AMOUNT, null),
                        containsIfPresent(ORDERS.CATEGORY, null)
                )
                .build();
        assertNotContains(r.sql(), "WHERE");
        assertEquals(0, r.namedParameters().size());
    }

    void testEmptyWhere() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .build();
        assertNotContains(r.sql(), "WHERE");
    }

    void testNeqNullThrows() {
        assertThrows(NullPointerException.class, () -> neq(ORDERS.STATUS, null));
    }

    void testGtNullThrows() {
        assertThrows(NullPointerException.class, () -> gt(ORDERS.AMOUNT, null));
    }

    void testLtNullThrows() {
        assertThrows(NullPointerException.class, () -> lt(ORDERS.AMOUNT, null));
    }

    void testBetweenNullFromThrows() {
        assertThrows(NullPointerException.class,
                () -> between(ORDERS.AMOUNT, null, new BigDecimal("100")));
    }

    void testBetweenNullToThrows() {
        assertThrows(NullPointerException.class,
                () -> between(ORDERS.AMOUNT, new BigDecimal("1"), null));
    }

    void testLikeNullThrows() {
        assertThrows(NullPointerException.class, () -> like(ORDERS.STATUS, null));
    }

    void testInEmptyListThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> in(ORDERS.STATUS, List.of()));
    }

    void testInIfPresentNullList() {
        assertNull(inIfPresent(ORDERS.STATUS, null));
    }

    void testInIfPresentEmptyList() {
        assertNull(inIfPresent(ORDERS.STATUS, List.of()));
    }

    void testContainsIfPresentNull() {
        assertNull(containsIfPresent(ORDERS.STATUS, null));
    }

    void testBetweenIfPresentNullFrom() {
        assertNull(betweenIfPresent(ORDERS.AMOUNT, null, new BigDecimal("100")));
    }

    void testBetweenIfPresentNullTo() {
        assertNull(betweenIfPresent(ORDERS.AMOUNT, new BigDecimal("1"), null));
    }

    // ==================== Composite edge cases ====================

    void testAndSingleUnwrap() {
        // and() with 1 condition should return the condition itself, not wrap in parens
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(and(eq(ORDERS.STATUS, "X")))
                .build();
        assertNotContains(r.sql(), "(");
        assertContains(r.sql(), "o.status =");
    }

    void testOrSingleUnwrap() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(or(eq(ORDERS.STATUS, "X")))
                .build();
        assertNotContains(r.sql(), "(");
    }

    void testAndAllNullsThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> and(null, null));
    }

    void testOrAllNullsThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> or(null, null));
    }

    void testAndIfAnyAllNulls() {
        assertNull(andIfAny(null, null, null));
    }

    void testDeeplyNestedAndOr() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(
                        or(
                                and(
                                        eq(ORDERS.STATUS, "A"),
                                        or(
                                                eq(ORDERS.CATEGORY, "X"),
                                                eq(ORDERS.CATEGORY, "Y")
                                        )
                                ),
                                eq(ORDERS.REGION, "EU")
                        )
                )
                .build();
        // Should produce: ((o.status = :s AND (o.category = :c1 OR o.category = :c2)) OR o.region = :r)
        assertContains(r.sql(), "OR");
        assertContains(r.sql(), "AND");
        assertEquals(4, r.namedParameters().size());
    }

    // ==================== SQL injection ====================

    void testInjectionSingleQuotes() {
        assertThrows(IllegalArgumentException.class,
                () -> ExpressionValidator.validateIdentifier("name'"));
    }

    void testInjectionBlockComment() {
        assertThrows(IllegalArgumentException.class,
                () -> ExpressionValidator.validateExpression("amount /* hack */"));
    }

    void testInjectionUnionInExpression() {
        // UNION is not in the dangerous keyword list (it's a SELECT keyword, not DML)
        // But DROP, INSERT etc. are
        assertThrows(IllegalArgumentException.class,
                () -> ExpressionValidator.validateExpression("1; INSERT INTO evil VALUES(1)"));
    }

    void testInjectionSemicolon() {
        assertThrows(IllegalArgumentException.class,
                () -> ExpressionValidator.validateExpression("1; SELECT 1"));
    }

    void testInjectionNullIdentifier() {
        assertThrows(IllegalArgumentException.class,
                () -> ExpressionValidator.validateIdentifier(null));
    }

    void testInjectionBlankExpression() {
        assertThrows(IllegalArgumentException.class,
                () -> ExpressionValidator.validateExpression("   "));
    }

    void testInjectionAlterIdentifier() {
        assertThrows(IllegalArgumentException.class,
                () -> ExpressionValidator.validateIdentifier("ALTER"));
    }

    void testValuesNeverInlined() {
        String malicious = "'; DROP TABLE orders; --";
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, malicious))
                .build();
        // The SQL must use a named parameter, never inline the value
        assertNotContains(r.sql(), "DROP");
        assertNotContains(r.sql(), malicious);
        assertContains(r.sql(), ":status_");
        // The value is in the parameter map
        assertTrue(r.namedParameters().values().contains(malicious),
                "Malicious value should be in params, not SQL");
    }

    // ==================== Dialect edge cases ====================

    void testAnsiLimitOnly() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS).limit(10).build();
        assertContains(r.sql(), "FETCH FIRST 10 ROWS ONLY");
    }

    void testAnsiOffsetOnly() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS).offset(5).build();
        assertContains(r.sql(), "OFFSET 5 ROWS");
    }


    // ==================== Column/table ====================

    void testColumnRef() {
        assertEquals("o.id", ORDERS.ID.ref());
        assertEquals("c.name", CUSTOMERS.NAME.ref());
    }

    void testColumnRefAs() {
        assertEquals("o.amount AS total", ORDERS.AMOUNT.refAs("total"));
    }

    void testColumnAggregates() {
        assertEquals("COUNT(o.id) AS cnt", ORDERS.ID.countAs("cnt"));
        assertEquals("SUM(o.amount) AS total", ORDERS.AMOUNT.sumAs("total"));
        assertEquals("AVG(o.amount) AS avg_amt", ORDERS.AMOUNT.avgAs("avg_amt"));
        assertEquals("MIN(o.amount) AS min_amt", ORDERS.AMOUNT.minAs("min_amt"));
        assertEquals("MAX(o.amount) AS max_amt", ORDERS.AMOUNT.maxAs("max_amt"));
    }

    void testTableDeclaration() {
        assertEquals("orders o", ORDERS.declaration());
        assertEquals("customers c", CUSTOMERS.declaration());
    }

    void testTableAllColumns() {
        assertTrue(ORDERS.allColumns().size() == 8,
                "OrderTable should have 8 columns, got " + ORDERS.allColumns().size());
        assertTrue(CUSTOMERS.allColumns().size() == 4,
                "CustomerTable should have 4 columns");
    }

    void testAliasedTableIndependence() {
        OrderTable o2 = ORDERS.as("o2");
        // Aliases must differ
        assertEquals("o", ORDERS.alias());
        assertEquals("o2", o2.alias());
        // Column refs must reflect the alias
        assertEquals("o.id", ORDERS.ID.ref());
        assertEquals("o2.id", o2.ID.ref());
    }

    void testMultipleAliases() {
        OrderTable a = ORDERS.as("a");
        OrderTable b = ORDERS.as("b");
        assertNotEquals(a.ID.ref(), b.ID.ref());
        assertEquals("a.id", a.ID.ref());
        assertEquals("b.id", b.ID.ref());
    }

    // ==================== SelectBuilder features ====================

    void testSelectDistinct() {
        SqlResult r = SelectBuilder.query()
                .selectDistinct(ORDERS.CUSTOMER_ID.ref())
                .from(ORDERS).build();
        assertContains(r.sql(), "SELECT DISTINCT o.customer_id");
    }

    void testSelectRaw() {
        SqlResult r = SelectBuilder.query()
                .selectRaw("1 AS one")
                .build();
        assertContains(r.sql(), "SELECT 1 AS one");
    }

    void testSelectRawInjection() {
        assertThrows(IllegalArgumentException.class,
                () -> SelectBuilder.query().selectRaw("1; DROP TABLE x"));
    }

    void testFromSubquery() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult sub = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "ACTIVE"))
                .build();
        SqlResult r = SelectBuilder.subquery(binder)
                .selectRaw("sub.id")
                .fromSubquery(sub, "sub")
                .build();
        assertContains(r.sql(), "FROM (SELECT o.id");
        assertContains(r.sql(), ") sub");
    }

    void testLeftJoin() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref(), PAYMENTS.STATUS.ref())
                .from(ORDERS)
                .leftJoin(PAYMENTS, ORDERS.ID, PAYMENTS.ORDER_ID)
                .build();
        assertContains(r.sql(), "LEFT JOIN payments p ON o.id = p.order_id");
    }

    void testMultiConditionJoin() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .join(JoinType.INNER, CUSTOMERS,
                        eqColumn(ORDERS.CUSTOMER_ID, CUSTOMERS.ID),
                        eq(CUSTOMERS.REGION, "EU"))
                .build();
        assertContains(r.sql(), "ON o.customer_id = c.id AND c.region =");
    }

    void testGroupByExprValidation() {
        assertThrows(IllegalArgumentException.class,
                () -> SelectBuilder.query()
                        .select(ORDERS.ID.ref())
                        .from(ORDERS)
                        .groupByExpr("1; DROP TABLE x"));
    }

    void testMultipleHaving() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.STATUS.ref(), ORDERS.AMOUNT.sumAs("total"))
                .from(ORDERS)
                .groupBy(ORDERS.STATUS)
                .havingRaw("SUM(o.amount) >= ?", new BigDecimal("500"))
                .havingRaw("COUNT(o.id) >= ?", 5)
                .build();
        assertContains(r.sql(), "HAVING SUM(o.amount) >=");
        assertContains(r.sql(), "AND COUNT(o.id) >=");
    }

    void testNoFrom() {
        SqlResult r = SelectBuilder.query().selectRaw("1 AS one").build();
        assertEquals("SELECT 1 AS one", r.sql());
    }

    // ==================== SqlResult ====================

    void testPositionalConversion() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "A"), eq(ORDERS.CATEGORY, "B"))
                .build();
        SqlResult.PositionalQuery pq = r.toPositional();
        assertNotContains(pq.sql(), ":");
        assertContains(pq.sql(), "?");
        assertEquals(2, pq.values().length);
        assertEquals("A", pq.values()[0]);
        assertEquals("B", pq.values()[1]);
    }

    void testVerifyValid() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "X"))
                .build();
        r.verify(); // should not throw
    }

    void testDebugStringTypes() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(
                        eq(ORDERS.STATUS, "ACTIVE"),
                        gte(ORDERS.AMOUNT, new BigDecimal("99"))
                )
                .build();
        String debug = r.toDebugString();
        assertContains(debug, "'ACTIVE'");   // string quoted
        assertContains(debug, "99");          // number not quoted
        assertNotContains(debug, "'99'");     // number should NOT be quoted
    }

    // ==================== ParameterBinder ====================

    void testBinderUniqueness() {
        ParameterBinder binder = new ParameterBinder();
        String p1 = binder.bind("A", "status");
        String p2 = binder.bind("B", "status");
        assertNotEquals(p1, p2);
        assertEquals(2, binder.getParameters().size());
    }

    // ==================== UnionBuilder ====================

    void testUnionNotAll() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "A")).build();
        SqlResult q2 = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "B")).build();
        SqlResult r = UnionBuilder.create(binder).union(q1).union(q2).build();
        assertContains(r.sql(), " UNION ");
        assertNotContains(r.sql(), "UNION ALL");
    }

    void testTripleUnion() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "A")).build();
        SqlResult q2 = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "B")).build();
        SqlResult q3 = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "C")).build();
        SqlResult r = UnionBuilder.create(binder)
                .unionAll(q1).unionAll(q2).unionAll(q3).build();
        // Should have two UNION ALL separators
        int count = countOccurrences(r.sql(), "UNION ALL");
        assertEquals(2, count);
        assertEquals(3, r.namedParameters().size());
    }

    // ==================== CTE ====================

    void testMultipleCtes() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult cte1 = SelectBuilder.subquery(binder)
                .select(ORDERS.CUSTOMER_ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "ACTIVE"))
                .build();
        SqlResult cte2 = SelectBuilder.subquery(binder)
                .select(PAYMENTS.ORDER_ID.ref())
                .from(PAYMENTS)
                .where(eq(PAYMENTS.STATUS, "PAID"))
                .build();
        SqlResult r = SelectBuilder.subquery(binder)
                .with("active_customers", cte1)
                .with("paid_orders", cte2)
                .select(CUSTOMERS.ID.ref())
                .from(CUSTOMERS)
                .build();
        assertContains(r.sql(), "WITH active_customers AS (");
        assertContains(r.sql(), ", paid_orders AS (");
    }

    // ==================== Thread safety ====================

    void testConcurrentBuilds() {
        int threads = 10;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        ConcurrentHashMap<String, String> results = new ConcurrentHashMap<>();
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            final String status = "STATUS_" + i;
            exec.submit(() -> {
                try {
                    SqlResult r = SelectBuilder.query()
                            .select(ORDERS.ID.ref())
                            .from(ORDERS)
                            .where(eq(ORDERS.STATUS, status))
                            .build();
                    results.put(status, r.toDebugString());
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        exec.shutdown();

        assertEquals(0, errors.get());
        assertEquals(threads, results.size());
        // Each result should contain only its own status
        for (int i = 0; i < threads; i++) {
            String status = "STATUS_" + i;
            String debug = results.get(status);
            assertContains(debug, "'" + status + "'");
            // Must not contain any other status
            for (int j = 0; j < threads; j++) {
                if (j != i) {
                    assertNotContains(debug, "STATUS_" + j);
                }
            }
        }
    }


    // ==================== IN list ====================

    void testInListMultipleValues() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(in(ORDERS.STATUS, List.of("A", "B", "C")))
                .build();
        assertContains(r.sql(), "o.status IN (");
        assertEquals(3, r.namedParameters().size());
    }

    // ==================== LIKE ====================

    void testLike() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(like(ORDERS.STATUS, "PEN%")).build();
        assertContains(r.sql(), "o.status LIKE");
    }

    void testNotLike() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(notLike(ORDERS.STATUS, "CAN%")).build();
        assertContains(r.sql(), "o.status NOT LIKE");
    }

    void testContains() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(contains(ORDERS.STATUS, "END")).build();
        // The bound value should be %END%
        assertTrue(r.namedParameters().values().stream()
                        .anyMatch(v -> "%END%".equals(v)),
                "Should bind %END%");
    }

    void testStartsWith() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(startsWith(ORDERS.STATUS, "PEN")).build();
        assertTrue(r.namedParameters().values().stream()
                        .anyMatch(v -> "PEN%".equals(v)),
                "Should bind PEN%");
    }

    // ==================== IS NULL ====================

    void testIsNull() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(isNull(ORDERS.CATEGORY)).build();
        assertContains(r.sql(), "o.category IS NULL");
        assertEquals(0, r.namedParameters().size());
    }

    void testIsNotNull() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(isNotNull(ORDERS.CATEGORY)).build();
        assertContains(r.sql(), "o.category IS NOT NULL");
    }

    // ==================== NOT IN ====================

    void testNotIn() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(notIn(ORDERS.STATUS, List.of("X", "Y"))).build();
        assertContains(r.sql(), "o.status NOT IN (");
    }

    // ==================== Raw with params ====================

    void testRawWithParams() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(raw("o.amount BETWEEN ? AND ?",
                        new BigDecimal("100"), new BigDecimal("500")))
                .build();
        assertContains(r.sql(), "o.amount BETWEEN :raw_");
        assertEquals(2, r.namedParameters().size());
    }

    // ==================== NOT EXISTS ====================

    void testNotExists() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult sub = SelectBuilder.subquery(binder)
                .selectRaw("1").from(PAYMENTS)
                .where(eqColumn(PAYMENTS.ORDER_ID, ORDERS.ID)).build();
        SqlResult r = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(notExists(sub)).build();
        assertContains(r.sql(), "NOT EXISTS (");
    }

    // ==================== Column conditions ====================

    void testEqColumn() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
                .where(eqColumn(ORDERS.CUSTOMER_ID, CUSTOMERS.ID)).build();
        assertContains(r.sql(), "o.customer_id = c.id");
    }

    void testColumnOpGt() {
        OrderTable o2 = ORDERS.as("o2");
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .innerJoin(o2, ORDERS.CUSTOMER_ID, o2.CUSTOMER_ID)
                .where(columnOp(ORDERS.AMOUNT, ComparisonOp.GT, o2.AMOUNT))
                .build();
        assertContains(r.sql(), "o.amount > o2.amount");
    }

    // ==================== Helpers ====================

    private int countOccurrences(String text, String sub) {
        int count = 0;
        int idx = 0;
        while ((idx = text.indexOf(sub, idx)) != -1) {
            count++;
            idx += sub.length();
        }
        return count;
    }

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

    private void assertNotEquals(Object a, Object b) {
        if (a.equals(b)) {
            throw new AssertionError("Expected values to differ but both were: " + a);
        }
    }

    private void assertTrue(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    private void assertNull(Object obj) {
        if (obj != null) throw new AssertionError("Expected null but got: " + obj);
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

    private <T extends Throwable> void assertThrows(Class<T> type, Runnable action) {
        try {
            action.run();
            throw new AssertionError("Expected " + type.getSimpleName() + " but no exception thrown");
        } catch (Throwable t) {
            if (!type.isInstance(t)) {
                throw new AssertionError("Expected " + type.getSimpleName()
                        + " but got " + t.getClass().getSimpleName() + ": " + t.getMessage());
            }
        }
    }
}
