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

    // Oracle 12c+ uses ANSI FETCH FIRST; single impl, aliased for semantic clarity
    public static final SqlDialect ORACLE = ANSI;

}
