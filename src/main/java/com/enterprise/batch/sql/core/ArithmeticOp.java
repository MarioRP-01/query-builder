package com.enterprise.batch.sql.core;

public enum ArithmeticOp {
    ADD("+"), SUBTRACT("-"), MULTIPLY("*"), DIVIDE("/");

    private final String sql;

    ArithmeticOp(String sql) { this.sql = sql; }

    public String sql() { return sql; }
}
