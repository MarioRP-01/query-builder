package com.enterprise.batch.evaluation.domain;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;

public final class EvaluationGroupTable extends Table {

    public static final EvaluationGroupTable GROUPS = new EvaluationGroupTable("eg");

    public final Column<Long>   ID;
    public final Column<String> NAME;
    public final Column<String> STATUS;

    public EvaluationGroupTable(String alias) {
        super("evaluation_groups", alias);
        this.ID     = column("id", Long.class);
        this.NAME   = column("name", String.class);
        this.STATUS = column("status", String.class);
    }

    @Override
    public EvaluationGroupTable as(String newAlias) {
        return new EvaluationGroupTable(newAlias);
    }
}
