package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.sql.builder.UnionBuilder;
import com.enterprise.batch.sql.core.*;
import com.enterprise.batch.sql.debug.QueryDebugger;
import com.enterprise.batch.sql.param.ParameterBinder;
import com.enterprise.batch.sql.validation.ExpressionValidator;
import com.enterprise.batch.order.domain.CustomerTable;
import com.enterprise.batch.order.domain.OrderTable;
import com.enterprise.batch.order.domain.PaymentTable;
import com.enterprise.batch.order.domain.ProductTable;

import org.junit.jupiter.api.Test;

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
import static com.enterprise.batch.order.domain.OrderTable.ORDERS;
import static com.enterprise.batch.order.domain.CustomerTable.CUSTOMERS;
import static com.enterprise.batch.order.domain.PaymentTable.PAYMENTS;
import static com.enterprise.batch.order.domain.ProductTable.PRODUCTS;
import static org.assertj.core.api.Assertions.*;

/**
 * Edge-case and stress tests beyond the gap-specific tests.
 */
public class EdgeCaseTests {

    // ==================== Null/empty edge cases ====================

    @Test
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
        assertThat(r.sql()).doesNotContain("WHERE");
        assertThat(r.namedParameters().size()).isEqualTo(0);
    }

    @Test
    void testEmptyWhere() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .build();
        assertThat(r.sql()).doesNotContain("WHERE");
    }

    @Test
    void testNeqNullThrows() {
        assertThatThrownBy(() -> neq(ORDERS.STATUS, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testGtNullThrows() {
        assertThatThrownBy(() -> gt(ORDERS.AMOUNT, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testLtNullThrows() {
        assertThatThrownBy(() -> lt(ORDERS.AMOUNT, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testBetweenNullFromThrows() {
        assertThatThrownBy(() -> between(ORDERS.AMOUNT, null, new BigDecimal("100")))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testBetweenNullToThrows() {
        assertThatThrownBy(() -> between(ORDERS.AMOUNT, new BigDecimal("1"), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testLikeNullThrows() {
        assertThatThrownBy(() -> like(ORDERS.STATUS, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testInEmptyListThrows() {
        assertThatThrownBy(() -> in(ORDERS.STATUS, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInIfPresentNullList() {
        assertThat(inIfPresent(ORDERS.STATUS, null)).isNull();
    }

    @Test
    void testInIfPresentEmptyList() {
        assertThat(inIfPresent(ORDERS.STATUS, List.of())).isNull();
    }

    @Test
    void testContainsIfPresentNull() {
        assertThat(containsIfPresent(ORDERS.STATUS, null)).isNull();
    }

    @Test
    void testBetweenIfPresentNullFrom() {
        assertThat(betweenIfPresent(ORDERS.AMOUNT, null, new BigDecimal("100"))).isNull();
    }

    @Test
    void testBetweenIfPresentNullTo() {
        assertThat(betweenIfPresent(ORDERS.AMOUNT, new BigDecimal("1"), null)).isNull();
    }

    // ==================== Composite edge cases ====================

    @Test
    void testAndSingleUnwrap() {
        // and() with 1 condition should return the condition itself, not wrap in parens
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(and(eq(ORDERS.STATUS, "X")))
                .build();
        assertThat(r.sql()).doesNotContain("(");
        assertThat(r.sql()).contains("o.status =");
    }

    @Test
    void testOrSingleUnwrap() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(or(eq(ORDERS.STATUS, "X")))
                .build();
        assertThat(r.sql()).doesNotContain("(");
    }

    @Test
    void testAndAllNullsThrows() {
        assertThatThrownBy(() -> and(null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testOrAllNullsThrows() {
        assertThatThrownBy(() -> or(null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testAndIfAnyAllNulls() {
        assertThat(andIfAny(null, null, null)).isNull();
    }

    @Test
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
        assertThat(r.sql()).contains("OR");
        assertThat(r.sql()).contains("AND");
        assertThat(r.namedParameters().size()).isEqualTo(4);
    }

    // ==================== SQL injection ====================

    @Test
    void testInjectionSingleQuotes() {
        assertThatThrownBy(() -> ExpressionValidator.validateIdentifier("name'"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInjectionBlockComment() {
        assertThatThrownBy(() -> ExpressionValidator.validateExpression("amount /* hack */"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInjectionUnionInExpression() {
        // UNION is not in the dangerous keyword list (it's a SELECT keyword, not DML)
        // But DROP, INSERT etc. are
        assertThatThrownBy(() -> ExpressionValidator.validateExpression("1; INSERT INTO evil VALUES(1)"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInjectionSemicolon() {
        assertThatThrownBy(() -> ExpressionValidator.validateExpression("1; SELECT 1"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInjectionNullIdentifier() {
        assertThatThrownBy(() -> ExpressionValidator.validateIdentifier(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInjectionBlankExpression() {
        assertThatThrownBy(() -> ExpressionValidator.validateExpression("   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInjectionAlterIdentifier() {
        assertThatThrownBy(() -> ExpressionValidator.validateIdentifier("ALTER"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testValuesNeverInlined() {
        String malicious = "'; DROP TABLE orders; --";
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, malicious))
                .build();
        // The SQL must use a named parameter, never inline the value
        assertThat(r.sql()).doesNotContain("DROP");
        assertThat(r.sql()).doesNotContain(malicious);
        assertThat(r.sql()).contains(":status_");
        // The value is in the parameter map
        assertThat(r.namedParameters().values().contains(malicious))
                .as("Malicious value should be in params, not SQL").isTrue();
    }

    // ==================== Dialect edge cases ====================

    @Test
    void testAnsiLimitOnly() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS).limit(10).build();
        assertThat(r.sql()).contains("FETCH FIRST 10 ROWS ONLY");
    }

    @Test
    void testAnsiOffsetOnly() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS).offset(5).build();
        assertThat(r.sql()).contains("OFFSET 5 ROWS");
    }

    // ==================== Column/table ====================

    @Test
    void testColumnRef() {
        assertThat(ORDERS.ID.ref()).isEqualTo("o.id");
        assertThat(CUSTOMERS.NAME.ref()).isEqualTo("c.name");
    }

    @Test
    void testColumnRefAs() {
        assertThat(ORDERS.AMOUNT.refAs("total")).isEqualTo("o.amount AS total");
    }

    @Test
    void testColumnAggregates() {
        assertThat(Column.countAllAs("total")).isEqualTo("COUNT(*) AS total");
        assertThat(ORDERS.ID.countAs("cnt")).isEqualTo("COUNT(o.id) AS cnt");
        assertThat(ORDERS.AMOUNT.sumAs("total")).isEqualTo("SUM(o.amount) AS total");
        assertThat(ORDERS.AMOUNT.avgAs("avg_amt")).isEqualTo("AVG(o.amount) AS avg_amt");
        assertThat(ORDERS.AMOUNT.minAs("min_amt")).isEqualTo("MIN(o.amount) AS min_amt");
        assertThat(ORDERS.AMOUNT.maxAs("max_amt")).isEqualTo("MAX(o.amount) AS max_amt");
    }

    @Test
    void testTableDeclaration() {
        assertThat(ORDERS.declaration()).isEqualTo("orders o");
        assertThat(CUSTOMERS.declaration()).isEqualTo("customers c");
    }

    @Test
    void testTableAllColumns() {
        assertThat(ORDERS.allColumns().size())
                .as("OrderTable should have 8 columns, got " + ORDERS.allColumns().size()).isEqualTo(8);
        assertThat(CUSTOMERS.allColumns().size())
                .as("CustomerTable should have 4 columns").isEqualTo(4);
    }

    @Test
    void testAliasedTableIndependence() {
        OrderTable o2 = ORDERS.as("o2");
        // Aliases must differ
        assertThat(ORDERS.alias()).isEqualTo("o");
        assertThat(o2.alias()).isEqualTo("o2");
        // Column refs must reflect the alias
        assertThat(ORDERS.ID.ref()).isEqualTo("o.id");
        assertThat(o2.ID.ref()).isEqualTo("o2.id");
    }

    @Test
    void testMultipleAliases() {
        OrderTable a = ORDERS.as("a");
        OrderTable b = ORDERS.as("b");
        assertThat(a.ID.ref()).isNotEqualTo(b.ID.ref());
        assertThat(a.ID.ref()).isEqualTo("a.id");
        assertThat(b.ID.ref()).isEqualTo("b.id");
    }

    // ==================== SelectBuilder features ====================

    @Test
    void testSelectDistinct() {
        SqlResult r = SelectBuilder.query()
                .selectDistinct(ORDERS.CUSTOMER_ID.ref())
                .from(ORDERS).build();
        assertThat(r.sql()).contains("SELECT DISTINCT o.customer_id");
    }

    @Test
    void testSelectRaw() {
        SqlResult r = SelectBuilder.query()
                .selectRaw("1 AS one")
                .build();
        assertThat(r.sql()).contains("SELECT 1 AS one");
    }

    @Test
    void testSelectRawInjection() {
        assertThatThrownBy(() -> SelectBuilder.query().selectRaw("1; DROP TABLE x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
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
        assertThat(r.sql()).contains("FROM (SELECT o.id");
        assertThat(r.sql()).contains(") sub");
    }

    @Test
    void testLeftJoin() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref(), PAYMENTS.STATUS.ref())
                .from(ORDERS)
                .leftJoin(PAYMENTS, ORDERS.ID, PAYMENTS.ORDER_ID)
                .build();
        assertThat(r.sql()).contains("LEFT JOIN payments p ON o.id = p.order_id");
    }

    @Test
    void testMultiConditionJoin() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .join(JoinType.INNER, CUSTOMERS,
                        eqColumn(ORDERS.CUSTOMER_ID, CUSTOMERS.ID),
                        eq(CUSTOMERS.REGION, "EU"))
                .build();
        assertThat(r.sql()).contains("ON o.customer_id = c.id AND c.region =");
    }

    @Test
    void testGroupByExprValidation() {
        assertThatThrownBy(() -> SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .groupByExpr("1; DROP TABLE x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testMultipleHaving() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.STATUS.ref(), ORDERS.AMOUNT.sumAs("total"))
                .from(ORDERS)
                .groupBy(ORDERS.STATUS)
                .havingRaw("SUM(o.amount) >= ?", new BigDecimal("500"))
                .havingRaw("COUNT(o.id) >= ?", 5)
                .build();
        assertThat(r.sql()).contains("HAVING SUM(o.amount) >=");
        assertThat(r.sql()).contains("AND COUNT(o.id) >=");
    }

    @Test
    void testNoFrom() {
        SqlResult r = SelectBuilder.query().selectRaw("1 AS one").build();
        assertThat(r.sql()).isEqualTo("SELECT 1 AS one");
    }

    // ==================== SqlResult ====================

    @Test
    void testPositionalConversion() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "A"), eq(ORDERS.CATEGORY, "B"))
                .build();
        SqlResult.PositionalQuery pq = r.toPositional();
        assertThat(pq.sql()).doesNotContain(":");
        assertThat(pq.sql()).contains("?");
        assertThat(pq.values().length).isEqualTo(2);
        assertThat(pq.values()[0]).isEqualTo("A");
        assertThat(pq.values()[1]).isEqualTo("B");
    }

    @Test
    void testVerifyValid() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "X"))
                .build();
        r.verify(); // should not throw
    }

    @Test
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
        assertThat(debug).contains("'ACTIVE'");   // string quoted
        assertThat(debug).contains("99");          // number not quoted
        assertThat(debug).doesNotContain("'99'");  // number should NOT be quoted
    }

    // ==================== ParameterBinder ====================

    @Test
    void testBinderUniqueness() {
        ParameterBinder binder = new ParameterBinder();
        String p1 = binder.bind("A", "status");
        String p2 = binder.bind("B", "status");
        assertThat(p1).isNotEqualTo(p2);
        assertThat(binder.getParameters().size()).isEqualTo(2);
    }

    // ==================== UnionBuilder ====================

    @Test
    void testUnionNotAll() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult q1 = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "A")).build();
        SqlResult q2 = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(eq(ORDERS.STATUS, "B")).build();
        SqlResult r = UnionBuilder.create(binder).union(q1).union(q2).build();
        assertThat(r.sql()).contains(" UNION ");
        assertThat(r.sql()).doesNotContain("UNION ALL");
    }

    @Test
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
        assertThat(count).isEqualTo(2);
        assertThat(r.namedParameters().size()).isEqualTo(3);
    }

    // ==================== CTE ====================

    @Test
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
        assertThat(r.sql()).contains("WITH active_customers AS (");
        assertThat(r.sql()).contains(", paid_orders AS (");
    }

    // ==================== Thread safety ====================

    @Test
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

        assertThat(errors.get()).isEqualTo(0);
        assertThat(results.size()).isEqualTo(threads);
        // Each result should contain only its own status
        for (int i = 0; i < threads; i++) {
            String status = "STATUS_" + i;
            String debug = results.get(status);
            assertThat(debug).contains("'" + status + "'");
            // Must not contain any other status
            for (int j = 0; j < threads; j++) {
                if (j != i) {
                    assertThat(debug).doesNotContain("STATUS_" + j);
                }
            }
        }
    }

    // ==================== IN list ====================

    @Test
    void testInListMultipleValues() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(in(ORDERS.STATUS, List.of("A", "B", "C")))
                .build();
        assertThat(r.sql()).contains("o.status IN (");
        assertThat(r.namedParameters().size()).isEqualTo(3);
    }

    // ==================== LIKE ====================

    @Test
    void testLike() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(like(ORDERS.STATUS, "PEN%")).build();
        assertThat(r.sql()).contains("o.status LIKE");
    }

    @Test
    void testNotLike() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(notLike(ORDERS.STATUS, "CAN%")).build();
        assertThat(r.sql()).contains("o.status NOT LIKE");
    }

    @Test
    void testContains() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(contains(ORDERS.STATUS, "END")).build();
        // The bound value should be %END%
        assertThat(r.namedParameters().values().stream()
                .anyMatch(v -> "%END%".equals(v)))
                .as("Should bind %END%").isTrue();
    }

    @Test
    void testStartsWith() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(startsWith(ORDERS.STATUS, "PEN")).build();
        assertThat(r.namedParameters().values().stream()
                .anyMatch(v -> "PEN%".equals(v)))
                .as("Should bind PEN%").isTrue();
    }

    // ==================== IS NULL ====================

    @Test
    void testIsNull() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(isNull(ORDERS.CATEGORY)).build();
        assertThat(r.sql()).contains("o.category IS NULL");
        assertThat(r.namedParameters().size()).isEqualTo(0);
    }

    @Test
    void testIsNotNull() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(isNotNull(ORDERS.CATEGORY)).build();
        assertThat(r.sql()).contains("o.category IS NOT NULL");
    }

    // ==================== NOT IN ====================

    @Test
    void testNotIn() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(notIn(ORDERS.STATUS, List.of("X", "Y"))).build();
        assertThat(r.sql()).contains("o.status NOT IN (");
    }

    // ==================== Raw with params ====================

    @Test
    void testRawWithParams() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(raw("o.amount BETWEEN ? AND ?",
                        new BigDecimal("100"), new BigDecimal("500")))
                .build();
        assertThat(r.sql()).contains("o.amount BETWEEN :raw_");
        assertThat(r.namedParameters().size()).isEqualTo(2);
    }

    // ==================== NOT EXISTS ====================

    @Test
    void testNotExists() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult sub = SelectBuilder.subquery(binder)
                .selectRaw("1").from(PAYMENTS)
                .where(eqColumn(PAYMENTS.ORDER_ID, ORDERS.ID)).build();
        SqlResult r = SelectBuilder.subquery(binder)
                .select(ORDERS.ID.ref()).from(ORDERS)
                .where(notExists(sub)).build();
        assertThat(r.sql()).contains("NOT EXISTS (");
    }

    // ==================== Column conditions ====================

    @Test
    void testEqColumn() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
                .where(eqColumn(ORDERS.CUSTOMER_ID, CUSTOMERS.ID)).build();
        assertThat(r.sql()).contains("o.customer_id = c.id");
    }

    @Test
    void testColumnOpGt() {
        OrderTable o2 = ORDERS.as("o2");
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS)
                .innerJoin(o2, ORDERS.CUSTOMER_ID, o2.CUSTOMER_ID)
                .where(columnOp(ORDERS.AMOUNT, ComparisonOp.GT, o2.AMOUNT))
                .build();
        assertThat(r.sql()).contains("o.amount > o2.amount");
    }

    // ==================== Boolean → Integer conversion ====================

    @Test
    void testBooleanTrueConvertsToOne() {
        ParameterBinder binder = new ParameterBinder();
        binder.bind(true, "flag");
        assertThat(binder.getParameters().get("flag_1")).isEqualTo(1);
    }

    @Test
    void testBooleanFalseConvertsToZero() {
        ParameterBinder binder = new ParameterBinder();
        binder.bind(false, "flag");
        assertThat(binder.getParameters().get("flag_1")).isEqualTo(0);
    }

    @Test
    void testNonBooleanUnchanged() {
        ParameterBinder binder = new ParameterBinder();
        binder.bind("text", "col");
        binder.bind(42, "num");
        assertThat(binder.getParameters().get("col_1")).isEqualTo("text");
        assertThat(binder.getParameters().get("num_2")).isEqualTo(42);
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
}
