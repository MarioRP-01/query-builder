package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.*;
import com.enterprise.batch.sql.builder.MergeBuilder.ColumnValue;
import com.enterprise.batch.sql.core.ArithmeticOp;
import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.sql.expression.CaseExpression;
import com.enterprise.batch.sql.expression.Cases;
import com.enterprise.batch.sql.expression.SimpleCaseExpression;
import com.enterprise.batch.sql.param.ParameterBinder;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.order.domain.OrderTable.ORDERS;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for arithmetic SET expressions and CASE expressions.
 */
class ArithmeticAndCaseTests {

    // ==================== Arithmetic SET Tests ====================

    @Test
    void testSetAdd() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setAdd(ORDERS.AMOUNT, BigDecimal.TEN)
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertThat(r.sql()).contains("amount = amount + :amount_1");
    }

    @Test
    void testSetSubtract() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setSubtract(ORDERS.AMOUNT, BigDecimal.ONE)
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertThat(r.sql()).contains("amount = amount - :amount_1");
    }

    @Test
    void testSetMultiply() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setMultiply(ORDERS.AMOUNT, BigDecimal.valueOf(2))
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertThat(r.sql()).contains("amount = amount * :amount_1");
    }

    @Test
    void testSetDivide() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setDivide(ORDERS.AMOUNT, BigDecimal.valueOf(2))
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertThat(r.sql()).contains("amount = amount / :amount_1");
    }

    @Test
    void testSetColumnExpr() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setColumnExpr(ORDERS.AMOUNT, ORDERS.CUSTOMER_ID, ArithmeticOp.MULTIPLY, ORDERS.PRODUCT_ID)
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertThat(r.sql()).contains("amount = customer_id * product_id");
        assertThat(r.namedParameters().size()).isEqualTo(1); // only WHERE param
    }

    @Test
    void testArithmeticWithWhere() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setAdd(ORDERS.AMOUNT, BigDecimal.valueOf(50))
                .where(eq(ORDERS.STATUS, "PENDING"), gt(ORDERS.AMOUNT, BigDecimal.ZERO))
                .build();
        assertThat(r.sql()).contains("SET amount = amount + :amount_1");
        assertThat(r.sql()).contains("WHERE o.status = :status_2 AND o.amount > :amount_3");
    }

    @Test
    void testArithmeticBuildTemplate() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setAdd(ORDERS.AMOUNT, BigDecimal.ONE)
                .buildTemplate();
        assertThat(r.sql()).isEqualTo("UPDATE orders o SET amount = amount + :amount");
        assertThat(r.namedParameters().size()).isEqualTo(0);
    }

    @Test
    void testColumnExprBuildTemplate() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setColumnExpr(ORDERS.AMOUNT, ORDERS.CUSTOMER_ID, ArithmeticOp.MULTIPLY, ORDERS.PRODUCT_ID)
                .buildTemplate();
        assertThat(r.sql()).isEqualTo("UPDATE orders o SET amount = customer_id * product_id");
        assertThat(r.namedParameters().size()).isEqualTo(0);
    }

    @Test
    void testArithmeticInMerge() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(
                        new ColumnValue<>(ORDERS.ID, 1L),
                        new ColumnValue<>(ORDERS.AMOUNT, BigDecimal.TEN))
                .on(ORDERS.ID)
                .whenMatchedSetAdd(ORDERS.AMOUNT, BigDecimal.valueOf(100))
                .whenNotMatchedInsert(ORDERS.ID, ORDERS.AMOUNT)
                .build();
        assertThat(r.sql()).contains("o.amount = o.amount + :amount_");
    }

    @Test
    void testMixedSetClauses() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.STATUS, "PROCESSED")
                .setAdd(ORDERS.AMOUNT, BigDecimal.valueOf(10))
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertThat(r.sql()).contains("status = :status_1");
        assertThat(r.sql()).contains("amount = amount + :amount_2");
    }

    // ==================== CASE Expression Tests ====================

    @Test
    void testSearchedCase() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("High")
                .when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(100))).then("Medium")
                .orElse("Low");
        String sql = c.toSql(binder);
        assertThat(sql).contains("CASE WHEN o.amount > :amount_1 THEN :case_2");
        assertThat(sql).contains("WHEN o.amount > :amount_3 THEN :case_4");
        assertThat(sql).contains("ELSE :case_5 END");
        assertThat(binder.getParameters().size()).isEqualTo(5);
    }

    @Test
    void testSimpleCase() {
        ParameterBinder binder = new ParameterBinder();
        SimpleCaseExpression c = Cases.of(ORDERS.STATUS)
                .when("A").then("Active")
                .when("B").then("Blocked")
                .orElse("Unknown");
        String sql = c.toSql(binder);
        assertThat(sql).contains("CASE o.status");
        assertThat(sql).contains("WHEN :status_1 THEN :case_2");
        assertThat(sql).contains("WHEN :status_3 THEN :case_4");
        assertThat(sql).contains("ELSE :case_5 END");
        assertThat(binder.getParameters().size()).isEqualTo(5);
    }

    @Test
    void testCaseWithAlias() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("High")
                .orElse("Low")
                .as("tier");
        String sql = c.toSql(binder);
        assertThat(sql).contains("END AS tier");
    }

    @Test
    void testCaseNoElse() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("High")
                .end();
        String sql = c.toSql(binder);
        assertThat(sql).contains("CASE WHEN");
        assertThat(sql).contains("END");
        assertThat(sql).doesNotContain("ELSE");
    }

    @Test
    void testCaseInSelect() {
        CaseExpression caseExpr = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("High")
                .orElse("Low")
                .as("tier");
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(caseExpr)
                .from(ORDERS)
                .build();
        assertThat(r.sql()).contains("SELECT o.id, CASE WHEN");
        assertThat(r.sql()).contains("END AS tier");
    }

    @Test
    void testCaseInUpdateSet() {
        CaseExpression caseExpr = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("HIGH")
                .orElse("LOW");
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setCase(ORDERS.CATEGORY, caseExpr)
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertThat(r.sql()).contains("category = CASE WHEN");
        assertThat(r.sql()).contains("END");
    }

    @Test
    void testCaseInOrderBy() {
        CaseExpression caseExpr = Cases.when(eq(ORDERS.STATUS, "URGENT")).then(1)
                .when(eq(ORDERS.STATUS, "NORMAL")).then(2)
                .orElse(3);
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.STATUS.ref())
                .from(ORDERS)
                .orderByExpr(caseExpr, SortDirection.ASC)
                .build();
        assertThat(r.sql()).contains("ORDER BY CASE WHEN");
        assertThat(r.sql()).contains("ASC");
    }

    @Test
    void testCaseMultipleWhens() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(10000))).then("Platinum")
                .when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(5000))).then("Gold")
                .when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("Silver")
                .orElse("Bronze");
        String sql = c.toSql(binder);
        // 3 WHEN branches
        int count = sql.split("WHEN").length - 1;
        assertThat(count).isEqualTo(3);
    }

    @Test
    void testCaseWithConditions() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(
                and(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000)), eq(ORDERS.STATUS, "ACTIVE")))
                .then("Premium")
                .orElse("Standard");
        String sql = c.toSql(binder);
        assertThat(sql).contains("AND");
    }

    @Test
    void testCaseParameterization() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(100))).then("A")
                .orElse("B");
        String sql = c.toSql(binder);
        // All values parameterized, none inlined
        assertThat(sql).doesNotContain("'A'");
        assertThat(sql).doesNotContain("'B'");
        assertThat(binder.getParameters().size()).isEqualTo(3);
    }

    @Test
    void testCaseInMerge() {
        CaseExpression caseExpr = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("HIGH")
                .orElse("LOW");
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(
                        new ColumnValue<>(ORDERS.ID, 1L),
                        new ColumnValue<>(ORDERS.AMOUNT, BigDecimal.TEN))
                .on(ORDERS.ID)
                .whenMatchedSetCase(ORDERS.CATEGORY, caseExpr)
                .whenNotMatchedInsert(ORDERS.ID, ORDERS.AMOUNT)
                .build();
        assertThat(r.sql()).contains("WHEN MATCHED THEN UPDATE SET o.category = CASE WHEN");
    }

    @Test
    void testSelectListRefactor() {
        // select() clears, selectExpr() appends
        SqlResult r1 = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.STATUS.ref())
                .from(ORDERS)
                .build();
        assertThat(r1.sql()).contains("SELECT o.id, o.status");

        // selectExpr appends after select
        CaseExpression caseExpr = Cases.when(eq(ORDERS.STATUS, "A")).then("Active")
                .orElse("Other").as("label");
        SqlResult r2 = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(caseExpr)
                .from(ORDERS)
                .build();
        assertThat(r2.sql()).contains("SELECT o.id, CASE");

        // select() after selectExpr clears everything
        SqlResult r3 = SelectBuilder.query()
                .selectExpr(caseExpr)
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .build();
        assertThat(r3.sql()).doesNotContain("CASE");
        assertThat(r3.sql()).contains("SELECT o.id");
    }
}
