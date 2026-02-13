package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.param.ParameterBinder;

@FunctionalInterface
public interface Condition {
    String toSql(ParameterBinder binder);
}
