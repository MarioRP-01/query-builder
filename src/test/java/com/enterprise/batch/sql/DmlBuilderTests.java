package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.*;
import com.enterprise.batch.sql.builder.MergeBuilder.ColumnValue;
import com.enterprise.batch.sql.param.ParameterBinder;
import com.enterprise.batch.spring.port.BatchDmlProvider;
import com.enterprise.batch.spring.adapter.DmlProviderRegistry;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.order.domain.OrderTable.ORDERS;
import static com.enterprise.batch.order.domain.CustomerTable.CUSTOMERS;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for DML builders (INSERT, UPDATE, DELETE, MERGE) and Spring integration.
 */
public class DmlBuilderTests {

    // ==================== InsertBuilder Tests ====================

    @Test
    void testInsertSingleRow() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .columns(ORDERS.ID, ORDERS.STATUS)
                .values(1001L, "PENDING")
                .build();

        assertThat(r.sql()).contains("INSERT INTO orders");
        assertThat(r.sql()).contains("(id, status)");
        assertThat(r.sql()).contains("VALUES (");
        assertThat(r.namedParameters().size()).isEqualTo(2);
        assertThat(r.toDebugString()).contains("1001");
        assertThat(r.toDebugString()).contains("'PENDING'");
    }

    @Test
    void testInsertMultiRow() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .columns(ORDERS.ID, ORDERS.STATUS)
                .values(1L, "A")
                .values(2L, "B")
                .values(3L, "C")
                .build();

        assertThat(r.sql()).contains("INSERT ALL");
        assertThat(r.sql()).contains("SELECT 1 FROM DUAL");
        // 3 rows x 2 columns = 6 params
        assertThat(r.namedParameters().size()).isEqualTo(6);
        // Should have 3 INTO clauses
        int count = r.sql().split(" INTO ").length - 1;
        assertThat(count).isEqualTo(3);
    }

    @Test
    void testInsertSetApi() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .set(ORDERS.ID, 42L)
                .set(ORDERS.STATUS, "NEW")
                .set(ORDERS.AMOUNT, BigDecimal.TEN)
                .build();

        assertThat(r.sql()).contains("INSERT INTO orders");
        assertThat(r.sql()).contains("id, status, amount");
        assertThat(r.namedParameters().size()).isEqualTo(3);
    }

    @Test
    void testInsertNullHandling() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .columns(ORDERS.ID, ORDERS.STATUS)
                .valuesOrNull(1L, null)
                .build();

        assertThat(r.sql()).contains("NULL");
        assertThat(r.namedParameters().size()).isEqualTo(1); // only the non-null id is bound
    }

    @Test
    void testInsertValuesRejectsNull() {
        assertThatThrownBy(() ->
                InsertBuilder.insert()
                        .into(ORDERS)
                        .columns(ORDERS.ID, ORDERS.STATUS)
                        .values(1L, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testInsertTemplate() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .columns(ORDERS.ID, ORDERS.AMOUNT, ORDERS.STATUS)
                .buildTemplate();

        assertThat(r.sql()).isEqualTo("INSERT INTO orders (id, amount, status) VALUES (:id, :amount, :status)");
        assertThat(r.namedParameters().size()).isEqualTo(0);
    }

    @Test
    void testInsertColumnMismatch() {
        assertThatThrownBy(() ->
                InsertBuilder.insert()
                        .into(ORDERS)
                        .columns(ORDERS.ID, ORDERS.STATUS)
                        .values(1L)) // only 1 value for 2 columns
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
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

        assertThat(r.sql()).contains("INSERT INTO customers");
        assertThat(r.sql()).contains("(id, name)");
        assertThat(r.sql()).contains("SELECT");
        assertThat(r.sql()).contains("FROM orders");
    }

    @Test
    void testInsertReturning() {
        SqlResult r = InsertBuilder.insert()
                .into(ORDERS)
                .set(ORDERS.ID, 1L)
                .set(ORDERS.STATUS, "NEW")
                .returning(ORDERS.ID)
                .build();

        assertThat(r.sql()).contains("RETURNING id INTO :id");
    }

    // ==================== UpdateBuilder Tests ====================

    @Test
    void testUpdateBasic() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.AMOUNT, BigDecimal.valueOf(500))
                .set(ORDERS.STATUS, "SHIPPED")
                .where(eq(ORDERS.ID, 1001L))
                .build();

        assertThat(r.sql()).contains("UPDATE orders o SET");
        assertThat(r.sql()).contains("amount = ");
        assertThat(r.sql()).contains("status = ");
        assertThat(r.sql()).contains("WHERE o.id = ");
        assertThat(r.namedParameters().size()).isEqualTo(3);
    }

    @Test
    void testUpdateSetIfPresent() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.STATUS, "DONE")
                .setIfPresent(ORDERS.CATEGORY, (String) null) // should be skipped
                .where(eq(ORDERS.ID, 1L))
                .build();

        assertThat(r.sql()).doesNotContain("category");
        assertThat(r.sql()).contains("status = ");
    }

    @Test
    void testUpdateSetNull() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .setNull(ORDERS.CATEGORY)
                .where(eq(ORDERS.ID, 1L))
                .build();

        assertThat(r.sql()).contains("category = NULL");
    }

    @Test
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

        assertThat(r.sql()).contains("category = (SELECT MAX(c.tier) FROM customers c");
    }

    @Test
    void testUpdateMissingWhereThrows() {
        assertThatThrownBy(() ->
                UpdateBuilder.update()
                        .table(ORDERS)
                        .set(ORDERS.STATUS, "X")
                        .build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testUpdateUnconditional() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.STATUS, "ARCHIVED")
                .buildUnconditional();

        assertThat(r.sql()).contains("UPDATE orders o SET");
        assertThat(r.sql()).doesNotContain("WHERE");
    }

    @Test
    void testUpdateTemplate() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.AMOUNT, BigDecimal.ONE)   // value ignored in template
                .set(ORDERS.STATUS, "X")              // value ignored in template
                .buildTemplate();

        assertThat(r.sql()).isEqualTo("UPDATE orders o SET amount = :amount, status = :status");
        assertThat(r.namedParameters().size()).isEqualTo(0);
    }

    @Test
    void testUpdateReturning() {
        SqlResult r = UpdateBuilder.update()
                .table(ORDERS)
                .set(ORDERS.STATUS, "DONE")
                .where(eq(ORDERS.ID, 1L))
                .returning(ORDERS.ID, ORDERS.STATUS)
                .build();

        assertThat(r.sql()).contains("RETURNING id, status INTO :id, :status");
    }

    // ==================== DeleteBuilder Tests ====================

    @Test
    void testDeleteBasic() {
        SqlResult r = DeleteBuilder.delete()
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, "CANCELLED"))
                .build();

        assertThat(r.sql()).contains("DELETE FROM orders o");
        assertThat(r.sql()).contains("WHERE o.status = ");
        assertThat(r.namedParameters().size()).isEqualTo(1);
    }

    @Test
    void testDeleteCompositeCondition() {
        SqlResult r = DeleteBuilder.delete()
                .from(ORDERS)
                .where(or(
                        eq(ORDERS.STATUS, "CANCELLED"),
                        eq(ORDERS.STATUS, "EXPIRED")))
                .build();

        assertThat(r.sql()).contains("DELETE FROM orders o WHERE");
        assertThat(r.sql()).contains("OR");
        assertThat(r.namedParameters().size()).isEqualTo(2);
    }

    @Test
    void testDeleteMissingWhereThrows() {
        assertThatThrownBy(() ->
                DeleteBuilder.delete()
                        .from(ORDERS)
                        .build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testDeleteUnconditional() {
        SqlResult r = DeleteBuilder.delete()
                .from(ORDERS)
                .buildUnconditional();

        assertThat(r.sql()).isEqualTo("DELETE FROM orders o");
        assertThat(r.namedParameters().size()).isEqualTo(0);
    }

    @Test
    void testDeleteReturning() {
        SqlResult r = DeleteBuilder.delete()
                .from(ORDERS)
                .where(eq(ORDERS.ID, 1L))
                .returning(ORDERS.ID)
                .build();

        assertThat(r.sql()).contains("RETURNING id INTO :id");
    }

    // ==================== MergeBuilder Tests ====================

    @Test
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

        assertThat(r.sql()).contains("MERGE INTO orders o");
        assertThat(r.sql()).contains("USING (SELECT");
        assertThat(r.sql()).contains("FROM DUAL) src");
        assertThat(r.sql()).contains("ON (o.id = src.id)");
        assertThat(r.sql()).contains("WHEN MATCHED THEN UPDATE SET o.amount = src.amount");
        assertThat(r.sql()).contains("WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (src.id, src.amount)");
    }

    @Test
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

        assertThat(r.sql()).contains("USING (SELECT");
        assertThat(r.sql()).contains(") stg");
        assertThat(r.sql()).contains("ON (o.id = stg.id)");
    }

    @Test
    void testMergeMatchedOnly() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(new ColumnValue<>(ORDERS.ID, 1L),
                           new ColumnValue<>(ORDERS.STATUS, "DONE"))
                .on(ORDERS.ID)
                .whenMatchedUpdate(ORDERS.STATUS)
                .build();

        assertThat(r.sql()).contains("WHEN MATCHED THEN UPDATE SET");
        assertThat(r.sql()).doesNotContain("WHEN NOT MATCHED");
    }

    @Test
    void testMergeNotMatchedOnly() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(new ColumnValue<>(ORDERS.ID, 1L),
                           new ColumnValue<>(ORDERS.STATUS, "NEW"))
                .on(ORDERS.ID)
                .whenNotMatchedInsert(ORDERS.ID, ORDERS.STATUS)
                .build();

        assertThat(r.sql()).doesNotContain("WHEN MATCHED");
        assertThat(r.sql()).contains("WHEN NOT MATCHED THEN INSERT");
    }

    @Test
    void testMergeBoth() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(new ColumnValue<>(ORDERS.ID, 1L),
                           new ColumnValue<>(ORDERS.AMOUNT, BigDecimal.ONE))
                .on(ORDERS.ID)
                .whenMatchedUpdate(ORDERS.AMOUNT)
                .whenNotMatchedInsert(ORDERS.ID, ORDERS.AMOUNT)
                .build();

        assertThat(r.sql()).contains("WHEN MATCHED THEN UPDATE SET");
        assertThat(r.sql()).contains("WHEN NOT MATCHED THEN INSERT");
    }

    @Test
    void testMergeMultiKeyOn() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(new ColumnValue<>(ORDERS.ID, 1L),
                           new ColumnValue<>(ORDERS.CUSTOMER_ID, 99L),
                           new ColumnValue<>(ORDERS.STATUS, "X"))
                .on(ORDERS.ID, ORDERS.CUSTOMER_ID)
                .whenMatchedUpdate(ORDERS.STATUS)
                .build();

        assertThat(r.sql()).contains("ON (o.id = src.id AND o.customer_id = src.customer_id)");
    }

    @Test
    void testMergeMatchedDelete() {
        SqlResult r = MergeBuilder.merge()
                .into(ORDERS)
                .usingDual(new ColumnValue<>(ORDERS.ID, 1L))
                .on(ORDERS.ID)
                .whenMatchedDelete()
                .build();

        assertThat(r.sql()).contains("WHEN MATCHED THEN DELETE");
    }

    @Test
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
        assertThat(r.sql()).contains("WHEN MATCHED THEN UPDATE SET o.status = :");
        assertThat(r.toDebugString()).contains("'UPDATED'");
    }

    // ==================== Spring Integration Tests ====================

    @Test
    void testRegistryRoundtrip() {
        DmlProviderRegistry registry = new DmlProviderRegistry();
        BatchDmlProvider provider = params -> UpdateBuilder.update()
                .table(ORDERS).set(ORDERS.STATUS, "X")
                .where(eq(ORDERS.ID, 1L)).build();
        registry.register("updateStatus", provider);

        BatchDmlProvider retrieved = registry.get("updateStatus");
        assertThat(retrieved == provider).as("Should return same provider instance").isTrue();
        assertThat(registry.all().size()).isEqualTo(1);
    }

    @Test
    void testRegistryUnknownThrows() {
        DmlProviderRegistry registry = new DmlProviderRegistry();
        assertThatThrownBy(() -> registry.get("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRegistryAllUnmodifiable() {
        DmlProviderRegistry registry = new DmlProviderRegistry();
        registry.register("x", params -> InsertBuilder.insert()
                .into(ORDERS).set(ORDERS.ID, 1L).build());
        assertThatThrownBy(() -> registry.all().put("hack", null))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
