package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.sql.core.NullsOrder;
import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.sql.expression.Frame;
import com.enterprise.batch.sql.expression.Over;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.enterprise.batch.order.domain.OrderTable.ORDERS;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for window (analytic) function expressions via {@link Over}.
 */
class WindowFunctionTests {

    // ==================== Aggregate functions ====================

    @Test
    void testSum() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.sum(ORDERS.AMOUNT)
                        .partitionBy(ORDERS.CUSTOMER_ID)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .as("running_total"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("SUM(o.amount) OVER (PARTITION BY o.customer_id ORDER BY o.created_date ASC) AS running_total");
    }

    @Test
    void testSumDistinct() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.sumDistinct(ORDERS.AMOUNT).partitionBy(ORDERS.CUSTOMER_ID).as("sum_d"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("SUM(DISTINCT o.amount) OVER (PARTITION BY o.customer_id) AS sum_d");
    }

    @Test
    void testCount() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.count(ORDERS.ID).partitionBy(ORDERS.CUSTOMER_ID).as("cnt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("COUNT(o.id) OVER (PARTITION BY o.customer_id) AS cnt");
    }

    @Test
    void testCountDistinct() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.countDistinct(ORDERS.STATUS).partitionBy(ORDERS.CUSTOMER_ID).as("cnt_d"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("COUNT(DISTINCT o.status) OVER (PARTITION BY o.customer_id) AS cnt_d");
    }

    @Test
    void testCountAll() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.countAll().partitionBy(ORDERS.REGION).as("region_cnt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("COUNT(*) OVER (PARTITION BY o.region) AS region_cnt");
    }

    @Test
    void testAvg() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.avg(ORDERS.AMOUNT).partitionBy(ORDERS.CUSTOMER_ID).as("avg_amt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("AVG(o.amount) OVER (PARTITION BY o.customer_id) AS avg_amt");
    }

    @Test
    void testAvgDistinct() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.avgDistinct(ORDERS.AMOUNT).partitionBy(ORDERS.CUSTOMER_ID).as("avg_d"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("AVG(DISTINCT o.amount) OVER (PARTITION BY o.customer_id) AS avg_d");
    }

    @Test
    void testMin() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.min(ORDERS.AMOUNT).partitionBy(ORDERS.CUSTOMER_ID).as("min_amt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("MIN(o.amount) OVER (PARTITION BY o.customer_id) AS min_amt");
    }

    @Test
    void testMax() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.max(ORDERS.AMOUNT).partitionBy(ORDERS.CUSTOMER_ID).as("max_amt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("MAX(o.amount) OVER (PARTITION BY o.customer_id) AS max_amt");
    }

    // ==================== Ranking functions ====================

    @Test
    void testRowNumber() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.rowNumber()
                        .partitionBy(ORDERS.CUSTOMER_ID)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.DESC)
                        .as("rn"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.created_date DESC) AS rn");
    }

    @Test
    void testRank() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.rank().orderBy(ORDERS.AMOUNT, SortDirection.DESC).as("rnk"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("RANK() OVER (ORDER BY o.amount DESC) AS rnk");
    }

    @Test
    void testDenseRank() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.denseRank().orderBy(ORDERS.AMOUNT, SortDirection.DESC).as("dr"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("DENSE_RANK() OVER (ORDER BY o.amount DESC) AS dr");
    }

    @Test
    void testNtile() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.ntile(4).orderBy(ORDERS.AMOUNT, SortDirection.DESC).as("quartile"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("NTILE(:ntile_1) OVER (ORDER BY o.amount DESC) AS quartile");
        assertThat(r.namedParameters()).containsEntry("ntile_1", 4);
    }

    // ==================== Value functions ====================

    @Test
    void testLagNoArgs() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.lag(ORDERS.AMOUNT)
                        .partitionBy(ORDERS.CUSTOMER_ID)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .as("prev_amt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("LAG(o.amount) OVER (PARTITION BY o.customer_id ORDER BY o.created_date ASC) AS prev_amt");
    }

    @Test
    void testLagWithOffset() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.lag(ORDERS.AMOUNT, 2)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .as("prev2"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("LAG(o.amount, :lag_1) OVER (ORDER BY o.created_date ASC) AS prev2");
        assertThat(r.namedParameters()).containsEntry("lag_1", 2);
    }

    @Test
    void testLagWithDefault() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.lag(ORDERS.AMOUNT, 1, BigDecimal.ZERO)
                        .partitionBy(ORDERS.CUSTOMER_ID)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .as("prev_amount"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("LAG(o.amount, :lag_1, :lag_2) OVER (PARTITION BY o.customer_id ORDER BY o.created_date ASC) AS prev_amount");
        assertThat(r.namedParameters()).containsEntry("lag_1", 1);
        assertThat(r.namedParameters()).containsEntry("lag_2", BigDecimal.ZERO);
    }

    @Test
    void testLeadNoArgs() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.lead(ORDERS.AMOUNT)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .as("next_amt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("LEAD(o.amount) OVER (ORDER BY o.created_date ASC) AS next_amt");
    }

    @Test
    void testLeadWithOffset() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.lead(ORDERS.AMOUNT, 3)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .as("next3"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("LEAD(o.amount, :lead_1) OVER (ORDER BY o.created_date ASC) AS next3");
        assertThat(r.namedParameters()).containsEntry("lead_1", 3);
    }

    @Test
    void testLeadWithDefault() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.lead(ORDERS.AMOUNT, 1, BigDecimal.ZERO)
                        .partitionBy(ORDERS.CUSTOMER_ID)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .as("next_amount"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("LEAD(o.amount, :lead_1, :lead_2) OVER (");
        assertThat(r.namedParameters()).containsEntry("lead_1", 1);
        assertThat(r.namedParameters()).containsEntry("lead_2", BigDecimal.ZERO);
    }

    @Test
    void testFirstValue() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.firstValue(ORDERS.AMOUNT)
                        .partitionBy(ORDERS.CUSTOMER_ID)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .as("first_amt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("FIRST_VALUE(o.amount) OVER (PARTITION BY o.customer_id ORDER BY o.created_date ASC) AS first_amt");
    }

    @Test
    void testLastValue() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.lastValue(ORDERS.AMOUNT)
                        .partitionBy(ORDERS.CUSTOMER_ID)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .frame(Frame.ROWS_BETWEEN_UNBOUNDED)
                        .as("last_amt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("LAST_VALUE(o.amount) OVER (PARTITION BY o.customer_id ORDER BY o.created_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amt");
    }

    @Test
    void testRatioToReport() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.ratioToReport(ORDERS.AMOUNT)
                        .partitionBy(ORDERS.REGION)
                        .as("pct"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("RATIO_TO_REPORT(o.amount) OVER (PARTITION BY o.region) AS pct");
    }

    // ==================== Frame clauses ====================

    @Test
    void testRowsUnboundedPreceding() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.sum(ORDERS.AMOUNT)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .frame(Frame.ROWS_UNBOUNDED_PRECEDING)
                        .as("rt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("SUM(o.amount) OVER (ORDER BY o.created_date ASC ROWS UNBOUNDED PRECEDING) AS rt");
    }

    @Test
    void testRangeUnboundedPreceding() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.sum(ORDERS.AMOUNT)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .frame(Frame.RANGE_UNBOUNDED_PRECEDING)
                        .as("rt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("RANGE UNBOUNDED PRECEDING) AS rt");
    }

    @Test
    void testRangeBetweenUnbounded() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.sum(ORDERS.AMOUNT)
                        .partitionBy(ORDERS.CUSTOMER_ID)
                        .frame(Frame.RANGE_BETWEEN_UNBOUNDED)
                        .as("total"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total");
    }

    @Test
    void testCustomFrame() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.avg(ORDERS.AMOUNT)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .frame(Frame.of("ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"))
                        .as("moving_avg"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg");
    }

    // ==================== Composition ====================

    @Test
    void testMultipleWindowExprsInSelect() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref())
                .selectExpr(
                        Over.rowNumber().partitionBy(ORDERS.CUSTOMER_ID).orderBy(ORDERS.CREATED_DATE, SortDirection.DESC).as("rn"),
                        Over.sum(ORDERS.AMOUNT).partitionBy(ORDERS.CUSTOMER_ID).as("cust_total"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.created_date DESC) AS rn");
        assertThat(r.sql()).contains("SUM(o.amount) OVER (PARTITION BY o.customer_id) AS cust_total");
    }

    @Test
    void testWindowExprInOrderBy() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.rowNumber().orderBy(ORDERS.AMOUNT, SortDirection.DESC).as("rn"))
                .from(ORDERS)
                .orderByExpr(Over.rowNumber().orderBy(ORDERS.AMOUNT, SortDirection.DESC).build(), SortDirection.ASC)
                .build();
        assertThat(r.sql()).contains("ORDER BY ROW_NUMBER() OVER (ORDER BY o.amount DESC) ASC");
    }

    // ==================== Edge cases ====================

    @Test
    void testEmptyOver() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.sum(ORDERS.AMOUNT).as("grand_total"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("SUM(o.amount) OVER () AS grand_total");
    }

    @Test
    void testOrderByWithNullsOrder() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.rowNumber()
                        .orderBy(ORDERS.AMOUNT, SortDirection.DESC, NullsOrder.NULLS_LAST)
                        .as("rn"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("ORDER BY o.amount DESC NULLS LAST) AS rn");
    }

    @Test
    void testMultiplePartitionAndOrderColumns() {
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(Over.sum(ORDERS.AMOUNT)
                        .partitionBy(ORDERS.CUSTOMER_ID, ORDERS.REGION)
                        .orderBy(ORDERS.CREATED_DATE, SortDirection.ASC)
                        .orderBy(ORDERS.ID, SortDirection.ASC)
                        .as("rt"))
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("PARTITION BY o.customer_id, o.region ORDER BY o.created_date ASC, o.id ASC) AS rt");
    }
}
