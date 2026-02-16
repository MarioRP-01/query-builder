package com.enterprise.batch.shared.querybridge.port;

import com.enterprise.batch.sql.builder.SqlResult;

import java.util.Map;

/**
 * Contract for Spring Batch DML providers.
 *
 * <p>Each call to {@link #buildDml(Map)} MUST create a fresh builder instance —
 * never reuse builder instances across calls (thread safety).
 *
 * <p>Register implementations as Spring beans:
 * <pre>{@code
 * @Bean("updateOrderStatus")
 * public BatchDmlProvider updateOrderStatusProvider() {
 *     return params -> UpdateBuilder.update()
 *         .table(ORDERS)
 *         .set(ORDERS.STATUS, (String) params.get("newStatus"))
 *         .where(eq(ORDERS.ID, (Long) params.get("orderId")))
 *         .build();
 * }
 * }</pre>
 */
@FunctionalInterface
public interface BatchDmlProvider {

    /**
     * Builds a DML statement using the given job parameters.
     * Must be stateless — create a new builder on every invocation.
     *
     * @param jobParams parameters from the Spring Batch job execution context
     * @return SqlResult ready for writer consumption
     */
    SqlResult buildDml(Map<String, Object> jobParams);
}
