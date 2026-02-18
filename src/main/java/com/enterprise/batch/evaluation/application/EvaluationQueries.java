package com.enterprise.batch.evaluation.application;

import com.enterprise.batch.shared.querybridge.port.BatchQueryProvider;
import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.core.SortDirection;

import static com.enterprise.batch.evaluation.domain.GroupElementTable.ELEMENTS;
import static com.enterprise.batch.sql.condition.Conditions.*;

public final class EvaluationQueries {

    private EvaluationQueries() {}

    public static BatchQueryProvider groupElements() {
        return params -> {
            Long groupId = (Long) params.get("groupId");
            return SelectBuilder.query()
                .select(ELEMENTS.ID.ref(), ELEMENTS.GROUP_ID.ref(),
                        ELEMENTS.ELEMENT_VALUE.ref(), ELEMENTS.CATEGORY.ref(),
                        ELEMENTS.CREATED_DATE.ref())
                .from(ELEMENTS)
                .where(eq(ELEMENTS.GROUP_ID, groupId))
                .orderBy(ELEMENTS.ID, SortDirection.ASC)
                .build();
        };
    }
}
