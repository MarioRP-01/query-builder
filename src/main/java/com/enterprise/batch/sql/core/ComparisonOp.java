package com.enterprise.batch.sql.core;

public enum ComparisonOp {
    EQ("="), NEQ("<>"), GT(">"), GTE(">="), LT("<"), LTE("<=");

    private final String sql;

    ComparisonOp(String sql) { this.sql = sql; }

    public String sql() { return sql; }
}
