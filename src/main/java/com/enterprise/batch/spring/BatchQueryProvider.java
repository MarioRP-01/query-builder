package com.enterprise.batch.spring;

import com.enterprise.batch.sql.builder.SqlResult;

import java.util.Map;

/**
 * Contract for Spring Batch query providers.
 * Each call to buildQuery() MUST create a fresh SelectBuilder (Gap #9: thread safety).
 */
public interface BatchQueryProvider {

    /**
     * Builds a query using the given job parameters.
     * Must be stateless — create a new SelectBuilder on every invocation.
     */
    SqlResult buildQuery(Map<String, Object> params);
}
