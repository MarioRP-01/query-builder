package com.enterprise.batch.shared.tasklets.adapter;

import com.enterprise.batch.shared.tasklets.port.BusinessDateResolver;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

/**
 * Tasklet that resolves business dates and writes them to the
 * {@link org.springframework.batch.core.JobExecution} execution context
 * so downstream steps can read them.
 *
 * <p>Writes two keys: {@code "lastWeekday"} and {@code "lastBusinessDay"}.
 */
public class BusinessDateTasklet implements Tasklet {

    private final BusinessDateResolver resolver;

    public BusinessDateTasklet(BusinessDateResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        var ctx = chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext();
        ctx.put("lastWeekday", resolver.lastWeekday());
        ctx.put("lastBusinessDay", resolver.lastBusinessDay());
        return RepeatStatus.FINISHED;
    }
}
