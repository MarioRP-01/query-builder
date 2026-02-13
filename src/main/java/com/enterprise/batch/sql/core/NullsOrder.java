package com.enterprise.batch.sql.core;

/**
 * NULLS FIRST / NULLS LAST ordering for ORDER BY clauses.
 */
public enum NullsOrder {
    NULLS_FIRST("NULLS FIRST"),
    NULLS_LAST("NULLS LAST");

    private final String sql;

    NullsOrder(String sql) {
        this.sql = sql;
    }

    public String sql() {
        return sql;
    }
}
