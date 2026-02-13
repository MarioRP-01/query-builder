package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.*;
import com.enterprise.batch.sql.builder.MergeBuilder.ColumnValue;
import com.enterprise.batch.sql.param.ParameterBinder;
import com.enterprise.batch.spring.BatchDmlProvider;
import com.enterprise.batch.spring.DmlProviderRegistry;

import java.math.BigDecimal;
import java.util.Map;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.example.tables.OrderTable.ORDERS;
import static com.enterprise.batch.example.tables.CustomerTable.CUSTOMERS;

/**
 * Tests for DML builders (INSERT, UPDATE, DELETE, MERGE) and Spring integration.
 */
public class DmlBuilderTests {

    private int passed = 0;
    private int failed = 0;

    public static void main(String[] args) {
        DmlBuilderTests t = new DmlBuilderTests();
        t.runAll();
    }

    public void runAll() {
        System.out.println("=== Running DML Builder Tests ===\n");

        // InsertBuilder
        test("Insert: single row with columns/values", this::testInsertSingleRow);
        test("Insert: multi-row INSERT ALL", this::testInsertMultiRow);
        test("Insert: set() API", this::testInsertSetApi);
        test("Insert: null handling (valuesOrNull)", this::testInsertNullHandling);
        test("Insert: values() rejects null", this::testInsertValuesRejectsNull);
        test("Insert: template mode", this::testInsertTemplate);
        test("Insert: column count mismatch throws", this::testInsertColumnMismatch);
        test("Insert: insertFrom (SELECT)", this::testInsertFrom);
        test("Insert: returning clause", this::testInsertReturning);

        // UpdateBuilder
        test("Update: basic SET + WHERE", this::testUpdateBasic);
        test("Update: setIfPresent skips null", this::testUpdateSetIfPresent);
        test("Update: setNull explicit", this::testUpdateSetNull);
        test("Update: setSubquery", this::testUpdateSetSubquery);
        test("Update: missing WHERE throws", this::testUpdateMissingWhereThrows);
        test("Update: unconditional", this::testUpdateUnconditional);
        test("Update: template mode", this::testUpdateTemplate);
        test("Update: returning clause", this::testUpdateReturning);

        // DeleteBuilder
        test("Delete: basic with condition", this::testDeleteBasic);
        test("Delete: composite conditions", this::testDeleteCompositeCondition);
        test("Delete: missing WHERE throws", this::testDeleteMissingWhereThrows);
        test("Delete: unconditional", this::testDeleteUnconditional);
        test("Delete: returning clause", this::testDeleteReturning);

        // MergeBuilder
        test("Merge: DUAL source upsert", this::testMergeDualSource);
        test("Merge: subquery source", this::testMergeSubquerySource);
        test("Merge: matched-only (update)", this::testMergeMatchedOnly);
        test("Merge: not-matched-only (insert)", this::testMergeNotMatchedOnly);
        test("Merge: both matched + not-matched", this::testMergeBoth);
        test("Merge: multi-key ON", this::testMergeMultiKeyOn);
        test("Merge: whenMatchedDelete", this::testMergeMatchedDelete);
        test("Merge: whenMatchedSet literal", this::testMergeMatchedSetLiteral);

        // Spring
        test("DmlProviderRegistry: register + get", this::testRegistryRoundtrip);
        test("DmlProviderRegistry: unknown throws", this::testRegistryUnknownThrows);
        test("DmlProviderRegistry: all() unmodifiable", this::testRegistryAllUnmodifiable);

        System.out.println("\n=== DML Builder Results: " + passed + " passed, " + failed + " failed ===");
        if (failed > 0) System.exit(1);
    }

    // ==================== InsertBuilder Tests ====================

    void testInsertSingleRow() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .columns(ORDERS.ID, ORDERS.STATUS)
                .values(1001L, "PENDING")
                .build();

        assertContains(r.sql(), "INSERT INTO orders");
        assertContains(r.sql(), "(id, status)");
        assertContains(r.sql(), "VALUES (");
        assertEquals(2, r.namedParameters().size());
        assertContains(r.toDebugString(), "1001");
        assertContains(r.toDebugString(), "'PENDING'");
    }

    void testInsertMultiRow() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .columns(ORDERS.ID, ORDERS.STATUS)
                .values(1L, "A")
                .values(2L, "B")
                .values(3L, "C")
                .build();

        assertContains(r.sql(), "INSERT ALL");
        assertContains(r.sql(), "SELECT 1 FROM DUAL");
        // 3 rows x 2 columns = 6 params
        assertEquals(6, r.namedParameters().size());
        // Should have 3 INTO clauses
        int count = r.sql().split(" INTO ").length - 1;
        assertEquals(3, count);
    }

    void testInsertSetApi() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .set(ORDERS.ID, 42L)
                .set(ORDERS.STATUS, "NEW")
                .set(ORDERS.AMOUNT, BigDecimal.TEN)
                .build();

        assertContains(r.sql(), "INSERT INTO orders");
        assertContains(r.sql(), "id, status, amount");
        assertEquals(3, r.namedParameters().size());
    }

    void testInsertNullHandling() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .columns(ORDERS.ID, ORDERS.STATUS)
                .valuesOrNull(1L, null)
                .build();

        assertContains(r.sql(), "NULL");
        assertEquals(1, r.namedParameters().size()); // only the non-null id is bound
    }

    void testInsertValuesRejectsNull() {
        assertThrows(NullPointerException.class, () ->
                InsertBuilder.insert()
                        .into(ORDERS)
                        .columns(ORDERS.ID, ORDERS.STATUS)
                        .values(1L, null));
    }

    void testInsertTemplate() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .columns(ORDERS.ID, ORDERS.AMOUNT, ORDERS.STATUS)
                .buildTemplate();

        assertEquals("INSERT INTO orders (id, amount, status) VALUES (:id, :amount, :status)",
                r.sql());
        assertEquals(0, r.namedParameters().size());
    }

    void testInsertColumnMismatch() {
        assertThrows(IllegalArgumentException.class, () ->
                InsertBuilder.insert()
                        .into(ORDERS)
                        .columns(ORDERS.ID, ORDERS.STATUS)
                        .values(1L)); // only 1 value for 2 columns
    }

    void testInsertFrom() {
        SqlResult selectResult = SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.STATUS.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "PENDING"))
                .build();

        SqlResult r = InsertBuilder.insert()
                .into(CUSTOMERS)
                .columns(CUSTOMERS.ID, CUSTOMERS.NAME)
                .insertFrom(selectResult)
                .build();

        assertContains(r.sql(), "INSERT INTO customers");
        assertContains(r.sql(), "(id, name)");
        assertContains(r.sql(), "SELECT");
        assertContains(r.sql(), "FROM orders");
    }

    void testInsertReturning() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .set(ORDERS.ID, 1L)
                .set(ORDERS.STATUS, "NEW")
                .returning(ORDERS.ID)
                .build();

        assertContains(r.sql(), "RETURNING id INTO :id");
    }

    // ==================== UpdateBuilder Tests ====================

    void testUpdateBasic() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.AMOUNT, BigDecimal.valueOf(500))
                .set(ORDERS.STATUS, "SHIPPED")
                .where(eq(ORDERS.ID, 1001L))
                .build();

        assertContains(r.sql(), "UPDATE orders o SET");
        assertContains(r.sql(), "amount = ");
        assertContains(r.sql(), "status = ");
        assertContains(r.sql(), "WHERE o.id = ");
        assertEquals(3, r.namedParameters().size());
    }

    void testUpdateSetIfPresent() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.STATUS, "DONE")
                .setIfPresent(ORDERS.CATEGORY, (String) null) // should be skipped
                .where(eq(ORDERS.ID, 1L))
                .build();

        assertNotContains(r.sql(), "category");
        assertContains(r.sql(), "status = ");
    }

    void testUpdateSetNull() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setNull(ORDERS.CATEGORY)
                .where(eq(ORDERS.ID, 1L))
                .build();

        assertContains(r.sql(), "category = NULL");
    }

    void testUpdateSetSubquery() {
        SqlResult subquery = SelectBuilder.query()
                .select("MAX(c.tier)")
                .from(CUSTOMERS)
                .where(eq(CUSTOMERS.ID, 99L))
                .build();

        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setSubquery(ORDERS.CATEGORY, subquery)
                .where(eq(ORDERS.ID, 1L))
                .build();

        assertContains(r.sql(), "category = (SELECT MAX(c.tier) FROM customers c");
    }

    void testUpdateMissingWhereThrows() {
        assertThrows(IllegalStateException.class, () ->
                UpdateBuilder.update()
                        .table(ORDERS)
                        .set(ORDERS.STATUS, "X")
                        .build());
    }

    void testUpdateUnconditional() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.STATUS, "ARCHIVED")
                .buildUnconditional();

        assertContains(r.sql(), "UPDATE orders o SET");
        assertNotContains(r.sql(), "WHERE");
    }

    void testUpdateTemplate() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.AMOUNT, BigDecimal.ONE)   // value ignored in template
                .set(ORDERS.STATUS, "X")              // value ignored in template
                .buildTemplate();

        assertEquals("UPDATE orders o SET amount = :amount, status = :status", r.sql());
        assertEquals(0, r.namedParameters().size());
    }

    void testUpdateReturning() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.STATUS, "DONE")
                .where(eq(ORDERS.ID, 1L))
                .returning(ORDERS.ID, ORDERS.STATUS)
                .build();

        assertContains(r.sql(), "RETURNING id, status INTO :id, :status");
    }

    // ==================== DeleteBuilder Tests ====================

    void testDeleteBasic() {
        SqlResult r = DeleteBuilder.delete()
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "CANCELLED"))
                .build();

        assertContains(r.sql(), "DELETE FROM orders o");
        assertContains(r.sql(), "WHERE o.status = ");
        assertEquals(1, r.namedParameters().size());
    }

    void testDeleteCompositeCondition() {
        SqlResult r = DeleteBuilder.delete()
                .from(ORDERS)
                .where(or(
                        eq(ORDERS.STATUS, "CANCELLED"),
                        eq(ORDERS.STATUS, "EXPIRED")))
                .build();

        assertContains(r.sql(), "DELETE FROM orders o WHERE");
        assertContains(r.sql(), "OR");
        assertEquals(2, r.namedParameters().size());
    }

    void testDeleteMissingWhereThrows() {
        assertThrows(IllegalStateException.class, () ->
                DeleteBuilder.delete()
                        .from(ORDERS)
                        .build());
    }

    void testDeleteUnconditional() {
        SqlResult r = DeleteBuilder.delete()
                .from(ORDERS)
                .buildUnconditional();

        assertEquals("DELETE FROM orders o", r.sql());
        assertEquals(0, r.namedParameters().size());
    }

    void testDeleteReturning() {
        SqlResult r = DeleteBuilder.delete()
                .from(ORDERS)
                .where(eq(ORDERS.ID, 1L))
                .returning(ORDERS.ID)
                .build();

        assertContains(r.sql(), "RETURNING id INTO :id");
    }

    // ==================== MergeBuilder Tests ====================

    void testMergeDualSource() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(
                        new ColumnValue<>(ORDERS.ID, 1001L),
                        new ColumnValue<>(ORDERS.AMOUNT, BigDecimal.TEN))
                .on(ORDERS.ID)
                .whenMatchedUpdate(ORDERS.AMOUNT)
                .whenNotMatchedInsert(ORDERS.ID, ORDERS.AMOUNT)
                .build();

        assertContains(r.sql(), "MERGE INTO orders o");
        assertContains(r.sql(), "USING (SELECT");
        assertContains(r.sql(), "FROM DUAL) src");
        assertContains(r.sql(), "ON (o.id = src.id)");
        assertContains(r.sql(), "WHEN MATCHED THEN UPDATE SET o.amount = src.amount");
        assertContains(r.sql(), "WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (src.id, src.amount)");
    }

    void testMergeSubquerySource() {
        SqlResult subquery = SelectBuilder.query()
                .select(CUSTOMERS.ID.ref(), CUSTOMERS.NAME.ref())
                .from(CUSTOMERS)
                .where(eq(CUSTOMERS.TIER, "GOLD"))
                .build();

        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingSubquery(subquery, "stg")
                .on(ORDERS.ID)
                .whenMatchedUpdate(ORDERS.STATUS)
                .build();

        assertContains(r.sql(), "USING (SELECT");
        assertContains(r.sql(), ") stg");
        assertContains(r.sql(), "ON (o.id = stg.id)");
    }

    void testMergeMatchedOnly() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(new ColumnValue<>(ORDERS.ID, 1L),
                           new ColumnValue<>(ORDERS.STATUS, "DONE"))
                .on(ORDERS.ID)
                .whenMatchedUpdate(ORDERS.STATUS)
                .build();

        assertContains(r.sql(), "WHEN MATCHED THEN UPDATE SET");
        assertNotContains(r.sql(), "WHEN NOT MATCHED");
    }

    void testMergeNotMatchedOnly() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(new ColumnValue<>(ORDERS.ID, 1L),
                           new ColumnValue<>(ORDERS.STATUS, "NEW"))
                .on(ORDERS.ID)
                .whenNotMatchedInsert(ORDERS.ID, ORDERS.STATUS)
                .build();

        assertNotContains(r.sql(), "WHEN MATCHED");
        assertContains(r.sql(), "WHEN NOT MATCHED THEN INSERT");
    }

    void testMergeBoth() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(new ColumnValue<>(ORDERS.ID, 1L),
                           new ColumnValue<>(ORDERS.AMOUNT, BigDecimal.ONE))
                .on(ORDERS.ID)
                .whenMatchedUpdate(ORDERS.AMOUNT)
                .whenNotMatchedInsert(ORDERS.ID, ORDERS.AMOUNT)
                .build();

        assertContains(r.sql(), "WHEN MATCHED THEN UPDATE SET");
        assertContains(r.sql(), "WHEN NOT MATCHED THEN INSERT");
    }

    void testMergeMultiKeyOn() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(new ColumnValue<>(ORDERS.ID, 1L),
                           new ColumnValue<>(ORDERS.CUSTOMER_ID, 99L),
                           new ColumnValue<>(ORDERS.STATUS, "X"))
                .on(ORDERS.ID, ORDERS.CUSTOMER_ID)
                .whenMatchedUpdate(ORDERS.STATUS)
                .build();

        assertContains(r.sql(), "ON (o.id = src.id AND o.customer_id = src.customer_id)");
    }

    void testMergeMatchedDelete() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(new ColumnValue<>(ORDERS.ID, 1L))
                .on(ORDERS.ID)
                .whenMatchedDelete()
                .build();

        assertContains(r.sql(), "WHEN MATCHED THEN DELETE");
    }

    void testMergeMatchedSetLiteral() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(new ColumnValue<>(ORDERS.ID, 1L),
                           new ColumnValue<>(ORDERS.AMOUNT, BigDecimal.TEN))
                .on(ORDERS.ID)
                .whenMatchedSet(ORDERS.STATUS, "UPDATED")
                .whenNotMatchedInsert(ORDERS.ID, ORDERS.AMOUNT)
                .build();

        // The STATUS is set to a literal, not from source
        assertContains(r.sql(), "WHEN MATCHED THEN UPDATE SET o.status = :");
        assertContains(r.toDebugString(), "'UPDATED'");
    }

    // ==================== Spring Integration Tests ====================

    void testRegistryRoundtrip() {
        DmlProviderRegistry registry = new DmlProviderRegistry();
        BatchDmlProvider provider = params -> UpdateBuilder.update()
                .table(ORDERS).set(ORDERS.STATUS, "X")
                .where(eq(ORDERS.ID, 1L)).build();
        registry.register("updateStatus", provider);

        BatchDmlProvider retrieved = registry.get("updateStatus");
        assertTrue(retrieved == provider, "Should return same provider instance");
        assertEquals(1, registry.all().size());
    }

    void testRegistryUnknownThrows() {
        DmlProviderRegistry registry = new DmlProviderRegistry();
        assertThrows(IllegalArgumentException.class, () -> registry.get("nonexistent"));
    }

    void testRegistryAllUnmodifiable() {
        DmlProviderRegistry registry = new DmlProviderRegistry();
        registry.register("x", params -> InsertBuilder.insert()
                .into(ORDERS).set(ORDERS.ID, 1L).build());
        assertThrows(UnsupportedOperationException.class,
                () -> registry.all().put("hack", null));
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

    private void assertTrue(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
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
