package com.enterprise.batch.sql.core;

public interface SqlDialect {
    String limit(int count);
    String offset(int skip);
    String limitOffset(int count, int skip);
}
