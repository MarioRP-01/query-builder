package com.enterprise.batch.sql;

import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.shared.querybridge.port.BatchQueryProvider;
import com.enterprise.batch.shared.querybridge.adapter.QueryProviderRegistry;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.order.domain.OrderTable.ORDERS;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the Spring Batch integration layer.
 *
 * <p>These tests verify the non-Spring parts of the integration
 * (provider contract, registry, query resolution). Actual
 * JdbcCursorItemReader creation requires a DataSource and is
 * tested in a Spring context integration test.
 */
class SpringBatchTests {

    @Test
    void testProviderFreshQuery() {
        BatchQueryProvider provider = params -> SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eqIfPresent(ORDERS.STATUS, (String) params.get("status")))
                .build();

        SqlResult r1 = provider.buildQuery(Map.of("status", "A"));
        SqlResult r2 = provider.buildQuery(Map.of("status", "B"));

        assertThat(r1.toDebugString()).contains("'A'");
        assertThat(r2.toDebugString()).contains("'B'");
        assertThat(r1.toDebugString()).doesNotContain("'B'");
    }

    @Test
    void testProviderVerifiesParams() {
        BatchQueryProvider provider = params -> SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eqIfPresent(ORDERS.STATUS, (String) params.get("status")))
                .build();

        SqlResult r = provider.buildQuery(Map.of("status", "PENDING"));
        r.verify(); // should not throw
        assertThat(r.sql()).contains("o.status =");
        assertThat(r.namedParameters().size()).isEqualTo(1);
    }

    @Test
    void testProviderBrokenParams() {
        // Provider that produces SQL with an unbound parameter reference
        BatchQueryProvider broken = params -> {
            String sql = "SELECT o.id FROM orders o WHERE o.status = :missing_param";
            return new SqlResult(sql, Map.of());
        };

        assertThatThrownBy(() -> broken.buildQuery(Map.of()).verify())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing_param");
    }

    @Test
    void testRegistryUnknownThrows() {
        QueryProviderRegistry registry = new QueryProviderRegistry();
        assertThatThrownBy(() -> registry.get("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRegistryRoundtrip() {
        QueryProviderRegistry registry = new QueryProviderRegistry();
        BatchQueryProvider provider = params -> SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS).build();
        registry.register("orders", provider);
        BatchQueryProvider retrieved = registry.get("orders");
        assertThat(retrieved).as("Should return same provider instance").isSameAs(provider);
        assertThat(registry.all().size()).isEqualTo(1);
    }

    @Test
    void testRegistryAllUnmodifiable() {
        QueryProviderRegistry registry = new QueryProviderRegistry();
        registry.register("x", params -> SelectBuilder.query()
                .select(ORDERS.ID.ref()).from(ORDERS).build());
        assertThatThrownBy(() -> registry.all().put("hack", null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testProviderOptionalParams() {
        BatchQueryProvider provider = params -> SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(
                        eqIfPresent(ORDERS.STATUS, (String) params.get("status")),
                        eqIfPresent(ORDERS.CATEGORY, (String) params.get("category"))
                )
                .build();

        // No params -> no WHERE
        SqlResult r = provider.buildQuery(Map.of());
        assertThat(r.sql()).doesNotContain("WHERE");

        // One param -> one condition
        SqlResult r2 = provider.buildQuery(Map.of("status", "PENDING"));
        assertThat(r2.sql()).contains("o.status =");
        assertThat(r2.sql()).doesNotContain("o.category");
    }

    @Test
    void testProviderPositionalOutput() {
        BatchQueryProvider provider = params -> SelectBuilder.query()
                .select(ORDERS.ID.ref())
                .from(ORDERS)
                .where(eq(ORDERS.STATUS, (String) params.get("status")))
                .build();

        SqlResult r = provider.buildQuery(Map.of("status", "ACTIVE"));
        SqlResult.PositionalQuery pq = r.toPositional();

        assertThat(pq.sql()).doesNotContain(":");
        assertThat(pq.sql()).contains("?");
        assertThat(pq.values().length).isEqualTo(1);
        assertThat(pq.values()[0]).isEqualTo("ACTIVE");
    }
}
