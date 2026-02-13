package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.spring.BatchQueryProvider;
import com.enterprise.batch.spring.QueryProviderRegistry;

import java.util.Map;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.example.tables.OrderTable.ORDERS;

/**
 * Tests for the Spring Batch integration layer.
 *
 * <p>These tests verify the non-Spring parts of the integration
 * (provider contract, registry, query resolution). Actual
 * JdbcCursorItemReader creation requires a DataSource and is
 * tested in a Spring context integration test.
 */
public class SpringBatchTests {

    private int passed = 0;
    private int failed = 0;

    public static void main(String[] args) {
        SpringBatchTests t = new SpringBatchTests();
        t.runAll();
    }

    public void runAll() {
        System.out.println("=== Running Spring Batch Tests ===\n");

        test("BatchQueryProvider builds fresh query each call", this::testProviderFreshQuery);
        test("Provider verifies query params", this::testProviderVerifiesParams);
        test("Provider detects broken params", this::testProviderBrokenParams);
        test("QueryProviderRegistry get unknown throws", this::testRegistryUnknownThrows);
        test("QueryProviderRegistry roundtrip", this::testRegistryRoundtrip);
        test("QueryProviderRegistry all() is unmodifiable", this::testRegistryAllUnmodifiable);
        test("Provider with optional params omits null conditions", this::testProviderOptionalParams);
        test("Provider produces positional-ready output", this::testProviderPositionalOutput);

        System.out.println("\n=== Spring Batch Results: " + passed + " passed, " + failed + " failed ===");
        if (failed > 0) System.exit(1);
    }

    void testProviderFreshQuery() {
        BatchQueryProvider provider = params -> SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eqIfPresent(ORDERS.STATUS, (String) params.get("status")))
                .build();

        SqlResult r1 = provider.buildQuery(Map.of("status", "A"));
        SqlResult r2 = provider.buildQuery(Map.of("status", "B"));

        assertContains(r1.toDebugString(), "'A'");
        assertContains(r2.toDebugString(), "'B'");
        assertNotContains(r1.toDebugString(), "'B'");
    }

    void testProviderVerifiesParams() {
        BatchQueryProvider provider = params -> SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eqIfPresent(ORDERS.STATUS, (String) params.get("status")))
                .build();

        SqlResult r = provider.buildQuery(Map.of("status", "PENDING"));
        r.verify(); // should not throw
        assertContains(r.sql(), "o.status =");
        assertEquals(1, r.namedParameters().size());
    }

    void testProviderBrokenParams() {
        // Provider that produces SQL with an unbound parameter reference
        BatchQueryProvider broken = params -> {
            String sql = "SELECT o.id FROM orders o WHERE o.status = :missing_param";
            return new SqlResult(sql, Map.of());
        };

        SqlResult r = broken.buildQuery(Map.of());
        boolean threw = false;
        try {
            r.verify();
        } catch (IllegalStateException e) {
            threw = true;
            assertContains(e.getMessage(), "missing_param");
        }
        assertTrue(threw, "Should throw on unbound parameter");
    }

    void testRegistryUnknownThrows() {
        QueryProviderRegistry registry = new QueryProviderRegistry();
        assertThrows(IllegalArgumentException.class, () -> registry.get("nonexistent"));
    }

    void testRegistryRoundtrip() {
        QueryProviderRegistry registry = new QueryProviderRegistry();
        BatchQueryProvider provider = params -> SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS).build();
        registry.register("orders", provider);
        BatchQueryProvider retrieved = registry.get("orders");
        assertTrue(retrieved == provider, "Should return same provider instance");
        assertEquals(1, registry.all().size());
    }

    void testRegistryAllUnmodifiable() {
        QueryProviderRegistry registry = new QueryProviderRegistry();
        registry.register("x", params -> SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS).build());
        assertThrows(UnsupportedOperationException.class,
                () -> registry.all().put("hack", null));
    }

    void testProviderOptionalParams() {
        BatchQueryProvider provider = params -> SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(
                        eqIfPresent(ORDERS.STATUS, (String) params.get("status")),
                        eqIfPresent(ORDERS.CATEGORY, (String) params.get("category"))
                )
                .build();

        // No params → no WHERE
        SqlResult r = provider.buildQuery(Map.of());
        assertNotContains(r.sql(), "WHERE");

        // One param → one condition
        SqlResult r2 = provider.buildQuery(Map.of("status", "PENDING"));
        assertContains(r2.sql(), "o.status =");
        assertNotContains(r2.sql(), "o.category");
    }

    void testProviderPositionalOutput() {
        BatchQueryProvider provider = params -> SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, (String) params.get("status")))
                .build();

        SqlResult r = provider.buildQuery(Map.of("status", "ACTIVE"));
        SqlResult.PositionalQuery pq = r.toPositional();

        assertNotContains(pq.sql(), ":");
        assertContains(pq.sql(), "?");
        assertEquals(1, pq.values().length);
        assertEquals("ACTIVE", pq.values()[0]);
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
