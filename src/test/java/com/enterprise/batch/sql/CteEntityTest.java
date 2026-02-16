package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.sql.core.*;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.math.BigDecimal;
import java.time.LocalDate;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import com.enterprise.batch.order.application.CustomerActivityQueries;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.order.application.ActiveOrdersCte.ACTIVE_ORDERS;
import static com.enterprise.batch.order.application.CustomerOrderDatesCte.CUSTOMER_DATES;
import static com.enterprise.batch.order.domain.OrderTable.ORDERS;
import static com.enterprise.batch.order.domain.CustomerTable.CUSTOMERS;
import static com.enterprise.batch.order.domain.PaymentTable.PAYMENTS;

/**
 * Tests for first-class {@link Cte} entity — fixed, parameterized, and dynamic modes.
 */
public class CteEntityTest {

    // ==================== CTE definitions ====================

    /** Fixed CTE — buildQuery() produces a complete query internally. */
    static final class HighValueCte extends Cte {
        static final HighValueCte HIGH_VALUE = new HighValueCte("hv");

        final Column<Long> CUSTOMER_ID;
        final Column<BigDecimal> TOTAL_AMOUNT;

        HighValueCte(String alias) {
            super("high_value_customers", alias);
            CUSTOMER_ID = column("customer_id", Long.class);
            TOTAL_AMOUNT = column("total_amount", BigDecimal.class);
        }

        @Override
        public SqlResult buildQuery(ParameterBinder binder) {
            return SelectBuilder.subquery(binder)
                    .select(ORDERS.CUSTOMER_ID.ref(),
                            ORDERS.AMOUNT.sumAs("total_amount"))
                    .from(ORDERS)
                    .where(gte(ORDERS.AMOUNT, new BigDecimal("1000")))
                    .groupBy(ORDERS.CUSTOMER_ID)
                    .build();
        }

        @Override
        public HighValueCte as(String newAlias) {
            return new HighValueCte(newAlias);
        }
    }

    /** Parameterized CTE — custom buildQuery with extra parameters. */
    static final class FilteredOrdersCte extends Cte {
        static final FilteredOrdersCte FILTERED = new FilteredOrdersCte("fo");

        final Column<Long> ID;
        final Column<String> STATUS;

        FilteredOrdersCte(String alias) {
            super("filtered_orders", alias);
            ID = column("id", Long.class);
            STATUS = column("status", String.class);
        }

        public SqlResult buildQuery(ParameterBinder binder, String status) {
            return SelectBuilder.subquery(binder)
                    .select(ORDERS.ID.ref(), ORDERS.STATUS.ref())
                    .from(ORDERS)
                    .where(eq(ORDERS.STATUS, status))
                    .build();
        }

        @Override
        public FilteredOrdersCte as(String newAlias) {
            return new FilteredOrdersCte(newAlias);
        }
    }

    /** Dynamic CTE — structural only, no built-in query. */
    static final class OrderSummaryCte extends Cte {
        static final OrderSummaryCte SUMMARY = new OrderSummaryCte("os");

        final Column<Long> CUSTOMER_ID;
        final Column<Long> ORDER_COUNT;

        OrderSummaryCte(String alias) {
            super("order_summary", alias);
            CUSTOMER_ID = column("customer_id", Long.class);
            ORDER_COUNT = column("order_count", Long.class);
        }

        @Override
        public OrderSummaryCte as(String newAlias) {
            return new OrderSummaryCte(newAlias);
        }
    }

    /** Second CTE for dependency tests. */
    static final class EnrichedCte extends Cte {
        static final EnrichedCte ENRICHED = new EnrichedCte("ec");

        final Column<Long> CUSTOMER_ID;
        final Column<String> NAME;
        final Column<BigDecimal> TOTAL_AMOUNT;

        EnrichedCte(String alias) {
            super("enriched_customers", alias);
            CUSTOMER_ID = column("customer_id", Long.class);
            NAME = column("name", String.class);
            TOTAL_AMOUNT = column("total_amount", BigDecimal.class);
        }

        @Override
        public EnrichedCte as(String newAlias) {
            return new EnrichedCte(newAlias);
        }
    }

    // Shortcuts
    static final HighValueCte HIGH_VALUE = HighValueCte.HIGH_VALUE;
    static final FilteredOrdersCte FILTERED = FilteredOrdersCte.FILTERED;
    static final OrderSummaryCte SUMMARY = OrderSummaryCte.SUMMARY;
    static final EnrichedCte ENRICHED = EnrichedCte.ENRICHED;

    // ==================== Tests ====================

    @Test
    void fixedCte_withCallsBuildQuery() {
        SqlResult result = SelectBuilder.query()
                .with(HIGH_VALUE)
                .select(HIGH_VALUE.CUSTOMER_ID.ref(), HIGH_VALUE.TOTAL_AMOUNT.ref())
                .from(HIGH_VALUE)
                .build();

        assertThat(result.sql()).startsWith("WITH high_value_customers AS (");
        assertThat(result.sql()).contains("SUM(o.amount) AS total_amount");
        assertThat(result.sql()).contains("o.amount >=");
        assertThat(result.namedParameters()).hasSize(1);
        assertThat(result.namedParameters().values()).contains(new BigDecimal("1000"));
    }

    @Test
    void parameterizedCte_explicitBuildQuery() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult result = SelectBuilder.subquery(binder)
                .with(FILTERED, FILTERED.buildQuery(binder, "ACTIVE"))
                .select(FILTERED.ID.ref())
                .from(FILTERED)
                .build();

        assertThat(result.sql()).contains("WITH filtered_orders AS (");
        assertThat(result.sql()).contains("o.status =");
        assertThat(result.namedParameters()).containsValue("ACTIVE");
    }

    @Test
    void dynamicCte_externalQuery() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult externalQuery = SelectBuilder.subquery(binder)
                .select(ORDERS.CUSTOMER_ID.ref(), ORDERS.ID.countAs("order_count"))
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "PENDING"))
                .groupBy(ORDERS.CUSTOMER_ID)
                .build();

        SqlResult result = SelectBuilder.subquery(binder)
                .with(SUMMARY, externalQuery)
                .select(SUMMARY.CUSTOMER_ID.ref(), SUMMARY.ORDER_COUNT.ref())
                .from(SUMMARY)
                .build();

        assertThat(result.sql()).contains("WITH order_summary AS (");
        assertThat(result.namedParameters()).containsValue("PENDING");
    }

    @Test
    void dynamicCte_throwsOnWithWithoutQuery() {
        assertThatThrownBy(() -> SelectBuilder.query().with(SUMMARY))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("order_summary");
    }

    @Test
    void cteInTypedJoin() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult summaryQuery = SelectBuilder.subquery(binder)
                .select(ORDERS.CUSTOMER_ID.ref(), ORDERS.ID.countAs("order_count"))
                .from(ORDERS)
                .groupBy(ORDERS.CUSTOMER_ID)
                .build();

        SqlResult result = SelectBuilder.subquery(binder)
                .with(SUMMARY, summaryQuery)
                .select(CUSTOMERS.ID.ref(), SUMMARY.ORDER_COUNT.ref())
                .from(CUSTOMERS)
                .join(JoinType.LEFT, SUMMARY, eqColumn(SUMMARY.CUSTOMER_ID, CUSTOMERS.ID))
                .build();

        assertThat(result.sql()).contains("LEFT JOIN order_summary os ON os.customer_id = c.id");
    }

    @Test
    void cteInFrom() {
        SqlResult result = SelectBuilder.query()
                .with(HIGH_VALUE)
                .select(HIGH_VALUE.CUSTOMER_ID.ref())
                .from(HIGH_VALUE)
                .build();

        assertThat(result.sql()).contains("FROM high_value_customers hv");
    }

    @Test
    void cteToCteDependency() {
        ParameterBinder binder = new ParameterBinder();

        SqlResult hvQuery = SelectBuilder.subquery(binder)
                .select(ORDERS.CUSTOMER_ID.ref(),
                        ORDERS.AMOUNT.sumAs("total_amount"))
                .from(ORDERS)
                .where(gte(ORDERS.AMOUNT, new BigDecimal("500")))
                .groupBy(ORDERS.CUSTOMER_ID)
                .build();

        // Second CTE references first CTE's columns
        SqlResult enrichedQuery = SelectBuilder.subquery(binder)
                .select(CUSTOMERS.ID.refAs("customer_id"),
                        CUSTOMERS.NAME.ref(),
                        HIGH_VALUE.TOTAL_AMOUNT.ref())
                .from(CUSTOMERS)
                .join(JoinType.INNER, HIGH_VALUE,
                        eqColumn(HIGH_VALUE.CUSTOMER_ID, CUSTOMERS.ID))
                .build();

        SqlResult result = SelectBuilder.subquery(binder)
                .with(HIGH_VALUE, hvQuery)
                .with(ENRICHED, enrichedQuery)
                .select(ENRICHED.NAME.ref(), ENRICHED.TOTAL_AMOUNT.ref())
                .from(ENRICHED)
                .build();

        assertThat(result.sql()).contains("WITH high_value_customers AS (");
        assertThat(result.sql()).contains("enriched_customers AS (");
        assertThat(result.sql()).contains("INNER JOIN high_value_customers hv");
    }

    @Test
    void cteAliasing() {
        HighValueCte aliased = HIGH_VALUE.as("x");

        assertThat(aliased.alias()).isEqualTo("x");
        assertThat(aliased.cteName()).isEqualTo("high_value_customers");
        assertThat(aliased.CUSTOMER_ID.ref()).isEqualTo("x.customer_id");
        assertThat(aliased.TOTAL_AMOUNT.ref()).isEqualTo("x.total_amount");
    }

    @Test
    void mixedCteApis() {
        ParameterBinder binder = new ParameterBinder();
        SqlResult rawCteQuery = SelectBuilder.subquery(binder)
                .select(ORDERS.CUSTOMER_ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "VIP"))
                .build();

        SqlResult result = SelectBuilder.subquery(binder)
                .with(HIGH_VALUE)
                .with("vip_customers", rawCteQuery)
                .select(HIGH_VALUE.CUSTOMER_ID.ref())
                .from(HIGH_VALUE)
                .build();

        assertThat(result.sql()).contains("WITH high_value_customers AS (");
        assertThat(result.sql()).contains("vip_customers AS (");
    }

    @Test
    void sharedBinderUniqueness() {
        ParameterBinder binder = new ParameterBinder();

        SqlResult result = SelectBuilder.subquery(binder)
                .with(HIGH_VALUE)
                .with(FILTERED, FILTERED.buildQuery(binder, "PENDING"))
                .select(HIGH_VALUE.CUSTOMER_ID.ref())
                .from(HIGH_VALUE)
                .build();

        assertThat(result.namedParameters()).hasSize(2);
        assertThat(result.namedParameters().values())
                .contains(new BigDecimal("1000"), "PENDING");
    }

    // ==================== Chained CTEs — production use case ====================

    @Test
    void customerActivityQueries_fixedAndParameterizedCtesChained() {
        SqlResult result = CustomerActivityQueries.customerOrderDates()
                .buildQuery(java.util.Map.of());

        // CTE 1 (fixed): active orders filter — hardcoded in ActiveOrdersCte.buildQuery
        assertThat(result.sql()).contains("WITH active_orders AS (");
        assertThat(result.sql()).contains("o.created_date IS NOT NULL");

        // CTE 2 (parameterized): defaults to excluding REFUNDED
        assertThat(result.sql()).contains("customer_order_dates AS (");
        assertThat(result.sql()).contains("INNER JOIN active_orders ao ON p.order_id = ao.id");
        assertThat(result.sql()).contains("GROUP BY ao.customer_id, ao.created_date");

        // Main query: LEFT JOIN second CTE, filter, order
        assertThat(result.sql()).contains("LEFT JOIN customer_order_dates cod ON c.id = cod.customer_id");
        assertThat(result.sql()).contains("ORDER BY c.id DESC");

        // 4 params: CANCELLED, INTERNAL, REFUNDED, INACTIVE
        assertThat(result.namedParameters()).hasSize(4);
        assertThat(result.namedParameters().values()).contains("CANCELLED", "REFUNDED", "INACTIVE");

        System.out.println("\n  Chained CTEs (default):\n  " + result.toDebugString());
    }

    @Test
    void customerActivityQueries_parameterizedCteAcceptsCustomStatus() {
        SqlResult result = CustomerActivityQueries.customerOrderDates()
                .buildQuery(java.util.Map.of("excludedPaymentStatus", "FAILED"));

        // Parameterized CTE uses the custom value instead of default "REFUNDED"
        assertThat(result.namedParameters().values()).contains("FAILED");
        assertThat(result.namedParameters().values()).doesNotContain("REFUNDED");

        System.out.println("\n  Chained CTEs (custom):\n  " + result.toDebugString());
    }

    @Test
    void customerActivityQueries_cteColumnsInSelectAndJoin() {
        SqlResult result = CustomerActivityQueries.customerOrderDates()
                .buildQuery(java.util.Map.of());

        // CTE columns referenced with proper alias in SELECT and JOIN
        assertThat(result.sql()).contains("cod.created_date");
        assertThat(result.sql()).contains("ao.customer_id");
    }
}
