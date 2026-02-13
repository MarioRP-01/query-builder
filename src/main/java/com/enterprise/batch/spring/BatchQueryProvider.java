package com.enterprise.batch.spring;

import com.enterprise.batch.sql.builder.SqlResult;

import java.util.Map;

/**
 * Contract for Spring Batch query providers.
 *
 * <p>Each call to {@link #buildQuery(Map)} MUST create a fresh
 * {@link com.enterprise.batch.sql.builder.SelectBuilder} — never reuse
 * builder instances across calls (thread safety).
 *
 * <p>Register implementations as Spring beans:
 * <pre>{@code
 * @Bean("pendingOrders")
 * public BatchQueryProvider pendingOrdersProvider() {
 *     return params -> SelectBuilder.query()
 *         .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref())
 *         .from(ORDERS)
 *         .where(eqIfPresent(ORDERS.STATUS, (String) params.get("status")))
 *         .build();
 * }
 * }</pre>
 */
@FunctionalInterface
public interface BatchQueryProvider {

    /**
     * Builds a query using the given job parameters.
     * Must be stateless — create a new SelectBuilder on every invocation.
     *
     * @param jobParams parameters from the Spring Batch job execution context
     * @return verified SqlResult ready for reader consumption
     */
    SqlResult buildQuery(Map<String, Object> jobParams);
}
