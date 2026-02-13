package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.*;
import com.enterprise.batch.sql.builder.MergeBuilder.ColumnValue;
import com.enterprise.batch.sql.core.ArithmeticOp;
import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.sql.expression.CaseExpression;
import com.enterprise.batch.sql.expression.Cases;
import com.enterprise.batch.sql.expression.SimpleCaseExpression;
import com.enterprise.batch.sql.param.ParameterBinder;

import java.math.BigDecimal;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.example.tables.OrderTable.ORDERS;

/**
 * Tests for arithmetic SET expressions and CASE expressions.
 */
public class ArithmeticAndCaseTests {

    private int passed = 0;
    private int failed = 0;

    public static void main(String[] args) {
        ArithmeticAndCaseTests t = new ArithmeticAndCaseTests();
        t.runAll();
    }

    public void runAll() {
        System.out.println("=== Running Arithmetic & CASE Tests ===\n");

        // Arithmetic SET
        test("Arithmetic: setAdd", this::testSetAdd);
        test("Arithmetic: setSubtract", this::testSetSubtract);
        test("Arithmetic: setMultiply", this::testSetMultiply);
        test("Arithmetic: setDivide", this::testSetDivide);
        test("Arithmetic: setColumnExpr", this::testSetColumnExpr);
        test("Arithmetic: with WHERE", this::testArithmeticWithWhere);
        test("Arithmetic: buildTemplate", this::testArithmeticBuildTemplate);
        test("Arithmetic: columnExpr buildTemplate", this::testColumnExprBuildTemplate);
        test("Arithmetic: in MERGE", this::testArithmeticInMerge);
        test("Arithmetic: mixed SET clauses", this::testMixedSetClauses);

        // CASE expressions
        test("CASE: searched", this::testSearchedCase);
        test("CASE: simple", this::testSimpleCase);
        test("CASE: with alias", this::testCaseWithAlias);
        test("CASE: no ELSE", this::testCaseNoElse);
        test("CASE: in SELECT", this::testCaseInSelect);
        test("CASE: in UPDATE SET", this::testCaseInUpdateSet);
        test("CASE: in ORDER BY", this::testCaseInOrderBy);
        test("CASE: multiple WHENs", this::testCaseMultipleWhens);
        test("CASE: with composite conditions", this::testCaseWithConditions);
        test("CASE: parameterization", this::testCaseParameterization);
        test("CASE: in MERGE", this::testCaseInMerge);
        test("SELECT: list refactor", this::testSelectListRefactor);

        System.out.println("\n=== Arithmetic & CASE Results: " + passed + " passed, " + failed + " failed ===");
        if (failed > 0) System.exit(1);
    }

    // ==================== Arithmetic SET Tests ====================

    void testSetAdd() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setAdd(ORDERS.AMOUNT, BigDecimal.TEN)
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertContains(r.sql(), "amount = amount + :amount_1");
    }

    void testSetSubtract() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setSubtract(ORDERS.AMOUNT, BigDecimal.ONE)
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertContains(r.sql(), "amount = amount - :amount_1");
    }

    void testSetMultiply() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setMultiply(ORDERS.AMOUNT, BigDecimal.valueOf(2))
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertContains(r.sql(), "amount = amount * :amount_1");
    }

    void testSetDivide() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setDivide(ORDERS.AMOUNT, BigDecimal.valueOf(2))
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertContains(r.sql(), "amount = amount / :amount_1");
    }

    void testSetColumnExpr() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setColumnExpr(ORDERS.AMOUNT, ORDERS.CUSTOMER_ID, ArithmeticOp.MULTIPLY, ORDERS.PRODUCT_ID)
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertContains(r.sql(), "amount = customer_id * product_id");
        assertEquals(1, r.namedParameters().size()); // only WHERE param
    }

    void testArithmeticWithWhere() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setAdd(ORDERS.AMOUNT, BigDecimal.valueOf(50))
                .where(eq(ORDERS.STATUS, "PENDING"), gt(ORDERS.AMOUNT, BigDecimal.ZERO))
                .build();
        assertContains(r.sql(), "SET amount = amount + :amount_1");
        assertContains(r.sql(), "WHERE o.status = :status_2 AND o.amount > :amount_3");
    }

    void testArithmeticBuildTemplate() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setAdd(ORDERS.AMOUNT, BigDecimal.ONE)
                .buildTemplate();
        assertEquals("UPDATE orders o SET amount = amount + :amount", r.sql());
        assertEquals(0, r.namedParameters().size());
    }

    void testColumnExprBuildTemplate() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setColumnExpr(ORDERS.AMOUNT, ORDERS.CUSTOMER_ID, ArithmeticOp.MULTIPLY, ORDERS.PRODUCT_ID)
                .buildTemplate();
        assertEquals("UPDATE orders o SET amount = customer_id * product_id", r.sql());
        assertEquals(0, r.namedParameters().size());
    }

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
        assertContains(r.sql(), "o.amount = o.amount + :amount_");
    }

    void testMixedSetClauses() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.STATUS, "PROCESSED")
                .setAdd(ORDERS.AMOUNT, BigDecimal.valueOf(10))
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertContains(r.sql(), "status = :status_1");
        assertContains(r.sql(), "amount = amount + :amount_2");
    }

    // ==================== CASE Expression Tests ====================

    void testSearchedCase() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("High")
                .when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(100))).then("Medium")
                .orElse("Low");
        String sql = c.toSql(binder);
        assertContains(sql, "CASE WHEN o.amount > :amount_1 THEN :case_2");
        assertContains(sql, "WHEN o.amount > :amount_3 THEN :case_4");
        assertContains(sql, "ELSE :case_5 END");
        assertEquals(5, binder.getParameters().size());
    }

    void testSimpleCase() {
        ParameterBinder binder = new ParameterBinder();
        SimpleCaseExpression c = Cases.of(ORDERS.STATUS)
                .when("A").then("Active")
                .when("B").then("Blocked")
                .orElse("Unknown");
        String sql = c.toSql(binder);
        assertContains(sql, "CASE o.status");
        assertContains(sql, "WHEN :status_1 THEN :case_2");
        assertContains(sql, "WHEN :status_3 THEN :case_4");
        assertContains(sql, "ELSE :case_5 END");
        assertEquals(5, binder.getParameters().size());
    }

    void testCaseWithAlias() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("High")
                .orElse("Low")
                .as("tier");
        String sql = c.toSql(binder);
        assertContains(sql, "END AS tier");
    }

    void testCaseNoElse() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("High")
                .end();
        String sql = c.toSql(binder);
        assertContains(sql, "CASE WHEN");
        assertContains(sql, "END");
        assertNotContains(sql, "ELSE");
    }

    void testCaseInSelect() {
        CaseExpression caseExpr = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("High")
                .orElse("Low")
                .as("tier");
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(caseExpr)
                .from(ORDERS)
                .build();
        assertContains(r.sql(), "SELECT o.id, CASE WHEN");
        assertContains(r.sql(), "END AS tier");
    }

    void testCaseInUpdateSet() {
        CaseExpression caseExpr = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("HIGH")
                .orElse("LOW");
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setCase(ORDERS.CATEGORY, caseExpr)
                .where(eq(ORDERS.ID, 1L))
                .build();
        assertContains(r.sql(), "category = CASE WHEN");
        assertContains(r.sql(), "END");
    }

    void testCaseInOrderBy() {
        CaseExpression caseExpr = Cases.when(eq(ORDERS.STATUS, "URGENT")).then(1)
                .when(eq(ORDERS.STATUS, "NORMAL")).then(2)
                .orElse(3);
        SqlResult r = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.STATUS.ref())
                .from(ORDERS)
                .orderByExpr(caseExpr, SortDirection.ASC)
                .build();
        assertContains(r.sql(), "ORDER BY CASE WHEN");
        assertContains(r.sql(), "ASC");
    }

    void testCaseMultipleWhens() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(10000))).then("Platinum")
                .when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(5000))).then("Gold")
                .when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000))).then("Silver")
                .orElse("Bronze");
        String sql = c.toSql(binder);
        // 3 WHEN branches
        int count = sql.split("WHEN").length - 1;
        assertEquals(3, count);
    }

    void testCaseWithConditions() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(
                and(gt(ORDERS.AMOUNT, BigDecimal.valueOf(1000)), eq(ORDERS.STATUS, "ACTIVE")))
                .then("Premium")
                .orElse("Standard");
        String sql = c.toSql(binder);
        assertContains(sql, "AND");
    }

    void testCaseParameterization() {
        ParameterBinder binder = new ParameterBinder();
        CaseExpression c = Cases.when(gt(ORDERS.AMOUNT, BigDecimal.valueOf(100))).then("A")
                .orElse("B");
        String sql = c.toSql(binder);
        // All values parameterized, none inlined
        assertNotContains(sql, "'A'");
        assertNotContains(sql, "'B'");
        assertEquals(3, binder.getParameters().size());
    }

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
        assertContains(r.sql(), "WHEN MATCHED THEN UPDATE SET o.category = CASE WHEN");
    }

    void testSelectListRefactor() {
        // select() clears, selectExpr() appends
        SqlResult r1 = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.STATUS.ref())
                .from(ORDERS)
                .build();
        assertContains(r1.sql(), "SELECT o.id, o.status");

        // selectExpr appends after select
        CaseExpression caseExpr = Cases.when(eq(ORDERS.STATUS, "A")).then("Active")
                .orElse("Other").as("label");
        SqlResult r2 = SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .selectExpr(caseExpr)
                .from(ORDERS)
                .build();
        assertContains(r2.sql(), "SELECT o.id, CASE");

        // select() after selectExpr clears everything
        SqlResult r3 = SelectBuilder.query()
                .selectExpr(caseExpr)
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .build();
        assertNotContains(r3.sql(), "CASE");
        assertContains(r3.sql(), "SELECT o.id");
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
}
