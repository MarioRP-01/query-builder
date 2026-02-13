package com.enterprise.batch.sql.core;

public final class Dialects {

    private Dialects() {}

    public static final SqlDialect ANSI = new SqlDialect() {
        @Override public String limit(int count) {
            return "FETCH FIRST " + count + " ROWS ONLY";
        }
        @Override public String offset(int skip) {
            return "OFFSET " + skip + " ROWS";
        }
        @Override public String limitOffset(int count, int skip) {
            return "OFFSET " + skip + " ROWS FETCH NEXT " + count + " ROWS ONLY";
        }
    };

    public static final SqlDialect ORACLE = new SqlDialect() {
        @Override public String limit(int count) {
            return "FETCH FIRST " + count + " ROWS ONLY";
        }
        @Override public String offset(int skip) {
            return "OFFSET " + skip + " ROWS";
        }
        @Override public String limitOffset(int count, int skip) {
            return "OFFSET " + skip + " ROWS FETCH NEXT " + count + " ROWS ONLY";
        }
    };

    public static final SqlDialect POSTGRESQL = new SqlDialect() {
        @Override public String limit(int count) {
            return "LIMIT " + count;
        }
        @Override public String offset(int skip) {
            return "OFFSET " + skip;
        }
        @Override public String limitOffset(int count, int skip) {
            return "LIMIT " + count + " OFFSET " + skip;
        }
    };

    public static final SqlDialect MYSQL = new SqlDialect() {
        @Override public String limit(int count) {
            return "LIMIT " + count;
        }
        @Override public String offset(int skip) {
            return "OFFSET " + skip;
        }
        @Override public String limitOffset(int count, int skip) {
            return "LIMIT " + count + " OFFSET " + skip;
        }
    };
}
