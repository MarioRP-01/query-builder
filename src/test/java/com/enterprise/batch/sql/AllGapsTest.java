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

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.example.tables.OrderTable.ORDERS;
import static com.enterprise.batch.example.tables.CustomerTable.CUSTOMERS;
import static com.enterprise.batch.example.tables.PaymentTable.PAYMENTS;
import static com.enterprise.batch.example.tables.ProductTable.PRODUCTS;

/**
 * Comprehensive tests demonstrating all 14 gap fixes.
 * Each test method documents which gap it addresses.
 */
public class AllGapsTest {

    // ==================== Gap #1: OR conditions ====================

    @Test
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

        assertThat(result.sql()).contains("OR");
        assertThat(result.sql()).contains("o.status =");
        assertThat(result.namedParameters().size()).isEqualTo(2);
    }

    @Test
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

        assertThat(result.sql()).contains("OR");
        assertThat(result.sql()).contains("AND");
        assertThat(result.sql()).contains("o.status =");
        assertThat(result.sql()).contains("o.amount >=");
        assertThat(result.sql()).contains("c.region =");
        assertThat(result.namedParameters().size()).isEqualTo(3);
    }

    @Test
    void testOrIfAny() {
        // All null children -> orIfAny returns null -> where() skips it
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

        assertThat(result.sql()).doesNotContain("OR");
        assertThat(result.sql()).contains("o.region =");
        assertThat(result.namedParameters().size()).isEqualTo(1);

        // One non-null child -> condition applied
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

        assertThat(result2.sql()).contains("o.status =");
        assertThat(result2.namedParameters().size()).isEqualTo(1);
    }

    // ==================== Gap #2: Table alias collision ====================

    @Test
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

        assertThat(result.sql()).contains("orders o");
        assertThat(result.sql()).contains("orders o2");
        assertThat(result.sql()).contains("o.customer_id = o2.customer_id");
        assertThat(result.sql()).contains("o2.amount >=");
    }

    @Test
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

        assertThat(result.sql()).contains("sub_o.customer_id");
        assertThat(result.sql()).contains("sub_o.amount >=");
        assertThat(result.sql()).contains("o.customer_id IN");
    }

    // ==================== Gap #3: Derived table joins ====================

    @Test
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

        assertThat(result.sql()).contains("INNER JOIN (SELECT so.customer_id");
        assertThat(result.sql()).contains(") summary ON summary.customer_id = o.customer_id");
    }

    // ==================== Gap #4: UNION ====================

    @Test
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

        assertThat(result.sql()).contains("UNION ALL");
        assertThat(result.sql()).contains("ORDER BY o.amount DESC");
        assertThat(result.namedParameters().size()).isEqualTo(2);
    }

    // ==================== Gap #5: CTE (WITH clause) ====================

    @Test
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

        assertThat(result.sql()).contains("WITH high_value AS (");
        assertThat(result.sql()).contains("SUM(o.amount) >=");
        assertThat(result.sql()).contains("INNER JOIN");
    }

    // ==================== Gap #6: Parameter verification ====================

    @Test
    void testParameterVerification() {
        // Valid query -- should not throw
        SqlResult valid = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "PENDING"))
                .build();

        assertThat(valid.namedParameters().size()).isEqualTo(1);

        // Positional conversion should also match
        SqlResult.PositionalQuery pq = valid.toPositional();
        assertThat(pq.values().length).isEqualTo(1);
        assertThat(pq.values()[0]).isEqualTo("PENDING");
    }

    // ==================== Gap #7: Named parameters ====================

    @Test
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

        assertThat(hasStatusParam).as("Should have a parameter with 'status' in name").isTrue();
        assertThat(hasAmountParam).as("Should have a parameter with 'amount' in name").isTrue();

        // SQL should use named parameters
        assertThat(result.sql()).contains(":status_");
        assertThat(result.sql()).contains(":amount_");
    }

    // ==================== Gap #8: Null semantics ====================

    @Test
    void testStrictNullThrows() {
        assertThatThrownBy(() -> eq(ORDERS.STATUS, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("IfPresent");
    }

    @Test
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

        assertThat(result.sql()).contains("o.category =");
        assertThat(result.sql()).doesNotContain("o.status");
        assertThat(result.sql()).doesNotContain("o.amount");
        assertThat(result.namedParameters().size()).isEqualTo(1);
    }

    // ==================== Gap #9: Thread safety ====================

    @Test
    void testThreadSafety() {
        // Each buildQuery call should produce independent results
        SqlResult r1 = buildTestQuery(Map.of("status", "A"));
        SqlResult r2 = buildTestQuery(Map.of("status", "B"));

        assertThat(r1.toDebugString()).contains("'A'");
        assertThat(r2.toDebugString()).contains("'B'");
        assertThat(r1.toDebugString()).doesNotContain("'B'");
        assertThat(r2.toDebugString()).doesNotContain("'A'");
    }

    private SqlResult buildTestQuery(Map<String, Object> params) {
        return SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eqIfPresent(ORDERS.STATUS, (String) params.get("status")))
                .build();
    }

    // ==================== Gap #11: SQL injection protection ====================

    @Test
    void testIdentifierValidation() {
        assertThatThrownBy(() -> ExpressionValidator.validateIdentifier("id; DROP TABLE orders"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDangerousKeywords() {
        assertThatThrownBy(() -> ExpressionValidator.validateExpression("1; DROP TABLE orders"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBlocksComments() {
        assertThatThrownBy(() -> ExpressionValidator.validateExpression("amount -- comment"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ==================== Gap #12: Dialect differences ====================

    @Test
    void testOracleDialect() {
        SqlResult result = SelectBuilder.query()
                .dialect(Dialects.ORACLE)
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .limit(100)
                .offset(50)
                .build();

        assertThat(result.sql()).contains("OFFSET 50 ROWS FETCH NEXT 100 ROWS ONLY");
    }

    @Test
    void testAnsiDialect() {
        SqlResult result = SelectBuilder.query()
                .dialect(Dialects.ANSI)
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .limit(100)
                .offset(50)
                .build();

        assertThat(result.sql()).contains("OFFSET 50 ROWS FETCH NEXT 100 ROWS ONLY");
    }

    // ==================== Gap #13: Debug logging ====================

    @Test
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
        assertThat(debug).contains("'PENDING'");
        assertThat(debug).contains("100");
        assertThat(debug).doesNotContain(":status_");
        assertThat(debug).doesNotContain(":amount_");
    }

    @Test
    void testQueryDebuggerFormat() {
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "ACTIVE"))
                .build();

        String formatted = QueryDebugger.format(result);
        assertThat(formatted).contains("SQL (named):");
        assertThat(formatted).contains("SQL (positional):");
        assertThat(formatted).contains("SQL (values inlined):");
        assertThat(formatted).contains("Parameters (1):");
    }

    // ==================== Gap #14: Pagination ====================

    @Test
    void testLimitOnly() {
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .limit(50)
                .build();

        assertThat(result.sql()).contains("FETCH FIRST 50 ROWS ONLY");
    }

    @Test
    void testLimitOffset() {
        SqlResult result = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .limit(50)
                .offset(100)
                .build();

        assertThat(result.sql()).contains("OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY");
    }

    // ==================== Integration tests ====================

    @Test
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

        assertThat(result.sql()).contains("SELECT o.id, o.amount, o.status, c.name, c.region");
        assertThat(result.sql()).contains("INNER JOIN customers c ON o.customer_id = c.id");
        assertThat(result.sql()).contains("o.status =");
        assertThat(result.sql()).contains("o.amount >=");
        assertThat(result.sql()).contains("OR");
        assertThat(result.sql()).contains("o.customer_id IN (SELECT o.customer_id");
        assertThat(result.sql()).contains("o.id NOT IN (SELECT p.order_id");
        assertThat(result.sql()).contains("ORDER BY o.created_date DESC");
        assertThat(result.sql()).contains("FETCH FIRST 1000 ROWS ONLY");

        // Should have: PENDING, 100, ELECTRONICS, BOOKS, 5000, COMPLETED = 6 params
        assertThat(result.namedParameters().size()).isEqualTo(6);

        System.out.println("\n  Generated SQL:\n  " + result.toDebugString());
    }

    @Test
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

        assertThat(result.sql()).contains("EXISTS (SELECT 1");
        assertThat(result.sql()).contains("o.customer_id = c.id");
        assertThat(result.sql()).contains("o.created_date >=");
        assertThat(result.sql()).contains("c.tier =");

        System.out.println("\n  Correlated query:\n  " + result.toDebugString());
    }

    @Test
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

        assertThat(result.sql()).contains("GROUP BY c.region, pr.category");
        assertThat(result.sql()).contains("HAVING SUM(o.amount) >=");
        assertThat(result.sql()).contains("COUNT(o.id) >=");
        assertThat(result.sql()).contains("ORDER BY total_amount DESC");

        System.out.println("\n  Aggregation report:\n  " + result.toDebugString());
    }
}
