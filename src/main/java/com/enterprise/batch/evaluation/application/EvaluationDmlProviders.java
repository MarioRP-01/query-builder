package com.enterprise.batch.evaluation.application;

import com.enterprise.batch.shared.querybridge.port.BatchDmlProvider;
import com.enterprise.batch.sql.builder.InsertBuilder;

import static com.enterprise.batch.evaluation.domain.FailedConditionTable.FAILED;

public final class EvaluationDmlProviders {

    private EvaluationDmlProviders() {}

    public static BatchDmlProvider insertFailedCondition() {
        return params -> InsertBuilder.insert()
            .into(FAILED)
            .columns(
                FAILED.ELEMENT_ID, FAILED.GROUP_ID,
                FAILED.CONDITION_CODE, FAILED.ELEMENT_VALUE,
                FAILED.THRESHOLD_VALUE, FAILED.EVALUATED_DATE)
            .buildTemplate();
    }
}
