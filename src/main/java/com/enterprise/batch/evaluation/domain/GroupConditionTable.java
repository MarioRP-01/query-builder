package com.enterprise.batch.evaluation.domain;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;

import java.math.BigDecimal;

public final class GroupConditionTable extends Table {

    public static final GroupConditionTable CONDITIONS = new GroupConditionTable("gc");

    public final Column<Long>       ID;
    public final Column<Long>       GROUP_ID;
    public final Column<String>     CONDITION_CODE;
    public final Column<String>     OPERATOR;
    public final Column<BigDecimal> THRESHOLD;
    public final Column<String>     AGGREGATE_KEY;

    public GroupConditionTable(String alias) {
        super("group_conditions", alias);
        this.ID             = column("id", Long.class);
        this.GROUP_ID       = column("group_id", Long.class);
        this.CONDITION_CODE = column("condition_code", String.class);
        this.OPERATOR       = column("operator", String.class);
        this.THRESHOLD      = column("threshold", BigDecimal.class);
        this.AGGREGATE_KEY  = column("aggregate_key", String.class);
    }

    @Override
    public GroupConditionTable as(String newAlias) {
        return new GroupConditionTable(newAlias);
    }
}
