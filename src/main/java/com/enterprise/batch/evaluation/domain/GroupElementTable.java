package com.enterprise.batch.evaluation.domain;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;

import java.math.BigDecimal;
import java.time.LocalDate;

public final class GroupElementTable extends Table {

    public static final GroupElementTable ELEMENTS = new GroupElementTable("ge");

    public final Column<Long>       ID;
    public final Column<Long>       GROUP_ID;
    public final Column<BigDecimal> ELEMENT_VALUE;
    public final Column<String>     CATEGORY;
    public final Column<LocalDate>  CREATED_DATE;

    public GroupElementTable(String alias) {
        super("group_elements", alias);
        this.ID           = column("id", Long.class);
        this.GROUP_ID     = column("group_id", Long.class);
        this.ELEMENT_VALUE = column("element_value", BigDecimal.class);
        this.CATEGORY     = column("category", String.class);
        this.CREATED_DATE = column("created_date", LocalDate.class);
    }

    @Override
    public GroupElementTable as(String newAlias) {
        return new GroupElementTable(newAlias);
    }
}
