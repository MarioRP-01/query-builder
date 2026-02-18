package com.enterprise.batch.evaluation.domain;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;

import java.math.BigDecimal;
import java.time.LocalDate;

public final class FailedConditionTable extends Table {

    public static final FailedConditionTable FAILED = new FailedConditionTable("fc");

    public final Column<Long>       ELEMENT_ID;
    public final Column<Long>       GROUP_ID;
    public final Column<String>     CONDITION_CODE;
    public final Column<BigDecimal> ELEMENT_VALUE;
    public final Column<BigDecimal> THRESHOLD_VALUE;
    public final Column<LocalDate>  EVALUATED_DATE;

    public FailedConditionTable(String alias) {
        super("failed_conditions", alias);
        this.ELEMENT_ID      = column("element_id", Long.class);
        this.GROUP_ID        = column("group_id", Long.class);
        this.CONDITION_CODE  = column("condition_code", String.class);
        this.ELEMENT_VALUE   = column("element_value", BigDecimal.class);
        this.THRESHOLD_VALUE = column("threshold_value", BigDecimal.class);
        this.EVALUATED_DATE  = column("evaluated_date", LocalDate.class);
    }

    @Override
    public FailedConditionTable as(String newAlias) {
        return new FailedConditionTable(newAlias);
    }
}
