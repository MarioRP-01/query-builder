package com.enterprise.batch.evaluation.infrastructure;

import com.enterprise.batch.evaluation.application.EvaluationDmlProviders;
import com.enterprise.batch.evaluation.application.EvaluationQueries;
import com.enterprise.batch.evaluation.application.GroupRunContext;
import com.enterprise.batch.evaluation.domain.ConditionRule;
import com.enterprise.batch.evaluation.domain.FailedConditionDto;
import com.enterprise.batch.evaluation.domain.GroupElementDto;
import com.enterprise.batch.shared.chunkutils.adapter.ExplodingItemReader;
import com.enterprise.batch.shared.querybridge.adapter.BatchReaderFactory;
import com.enterprise.batch.shared.querybridge.adapter.BatchWriterFactory;
import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.builder.SqlResult;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.enterprise.batch.evaluation.domain.EvaluationGroupTable.GROUPS;
import static com.enterprise.batch.evaluation.domain.GroupConditionTable.CONDITIONS;
import static com.enterprise.batch.evaluation.domain.GroupElementTable.ELEMENTS;
import static com.enterprise.batch.sql.condition.Conditions.*;

@Configuration
public class ConditionEvaluationJobConfig {

    // --- JobScope context ---

    @Bean
    @JobScope
    GroupRunContext groupRunContext() {
        return new GroupRunContext();
    }

    // --- Step 1: load groups, conditions, aggregates ---

    @Bean
    Step groupLoadingStep(JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            GroupRunContext groupRunContext,
            DataSource dataSource) {
        return new StepBuilder("groupLoadingStep", jobRepository)
            .tasklet((contribution, chunkContext) -> {
                var jdbc = new NamedParameterJdbcTemplate(dataSource);

                // 1. Active groups
                SqlResult groupQuery = SelectBuilder.query()
                    .select(GROUPS.ID.ref())
                    .from(GROUPS)
                    .where(eq(GROUPS.STATUS, "ACTIVE"))
                    .build();
                List<Long> activeIds = jdbc.query(
                    groupQuery.sql(), groupQuery.namedParameters(),
                    (rs, rowNum) -> rs.getLong("id"));
                activeIds.forEach(groupRunContext::addGroup);

                // 2. Conditions per group
                SqlResult condQuery = SelectBuilder.query()
                    .select(CONDITIONS.GROUP_ID.ref(),
                            CONDITIONS.CONDITION_CODE.ref(),
                            CONDITIONS.OPERATOR.ref(),
                            CONDITIONS.THRESHOLD.ref(),
                            CONDITIONS.AGGREGATE_KEY.ref())
                    .from(CONDITIONS)
                    .where(in(CONDITIONS.GROUP_ID, activeIds))
                    .build();
                jdbc.query(condQuery.sql(), condQuery.namedParameters(), (rs, rowNum) -> {
                    groupRunContext.addCondition(
                        rs.getLong("group_id"),
                        new ConditionRule(
                            rs.getString("condition_code"),
                            rs.getString("operator"),
                            rs.getBigDecimal("threshold"),
                            rs.getString("aggregate_key")));
                    return null;
                });

                // 3. Aggregates (AVG per group)
                SqlResult aggQuery = SelectBuilder.query()
                    .select(ELEMENTS.GROUP_ID.ref(),
                            "AVG(" + ELEMENTS.ELEMENT_VALUE.ref() + ") avg_value")
                    .from(ELEMENTS)
                    .where(in(ELEMENTS.GROUP_ID, activeIds))
                    .groupBy(ELEMENTS.GROUP_ID)
                    .build();
                jdbc.query(aggQuery.sql(), aggQuery.namedParameters(), (rs, rowNum) -> {
                    groupRunContext.setAggregates(
                        rs.getLong("group_id"),
                        Map.of("AVG_VALUE", rs.getBigDecimal("avg_value")));
                    return null;
                });

                return RepeatStatus.FINISHED;
            }, transactionManager)
            .build();
    }

    // --- Step 2: partitioned evaluation ---

    @Bean
    Partitioner evaluationPartitioner(GroupRunContext groupRunContext) {
        return gridSize -> {
            Map<String, ExecutionContext> partitions = new HashMap<>();
            List<Long> ids = groupRunContext.groupIds();
            for (int i = 0; i < ids.size(); i++) {
                ExecutionContext ec = new ExecutionContext();
                ec.putLong("groupId", ids.get(i));
                partitions.put("partition" + i, ec);
            }
            return partitions;
        };
    }

    @Bean
    @StepScope
    ExplodingItemReader<GroupElementDto, FailedConditionDto> conditionEvaluatingReader(
            @Value("#{stepExecutionContext['groupId']}") Long groupId,
            BatchReaderFactory factory,
            GroupRunContext groupRunContext) {
        var delegate = factory.cursorReader(
            "elementReader_" + groupId,
            EvaluationQueries.groupElements(),
            elementRowMapper(),
            Map.of("groupId", groupId));

        List<ConditionRule> conditions = groupRunContext.conditionsFor(groupId);
        Map<String, BigDecimal> aggregates = groupRunContext.aggregatesFor(groupId);

        return new ExplodingItemReader<>(delegate, element -> {
            List<FailedConditionDto> failures = new ArrayList<>();
            for (ConditionRule rule : conditions) {
                if (!rule.passes(element.value(), aggregates)) {
                    BigDecimal thresholdValue = rule.aggregateKey() != null
                            ? aggregates.get(rule.aggregateKey())
                            : rule.threshold();
                    failures.add(new FailedConditionDto(
                        element.id(), element.groupId(), rule.code(),
                        element.value(), thresholdValue, element.createdDate()));
                }
            }
            return failures;
        });
    }

    @Bean
    @StepScope
    JdbcBatchItemWriter<FailedConditionDto> failedConditionWriter(
            BatchWriterFactory factory) {
        return factory.templateWriter(
            "failedConditionWriter",
            EvaluationDmlProviders.insertFailedCondition(),
            item -> new MapSqlParameterSource()
                .addValue("element_id", item.elementId())
                .addValue("group_id", item.groupId())
                .addValue("condition_code", item.conditionCode())
                .addValue("element_value", item.elementValue())
                .addValue("threshold_value", item.thresholdValue())
                .addValue("evaluated_date", item.evaluatedDate()),
            Map.of());
    }

    @Bean
    Step evaluationSlaveStep(JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ExplodingItemReader<GroupElementDto, FailedConditionDto> conditionEvaluatingReader,
            JdbcBatchItemWriter<FailedConditionDto> failedConditionWriter) {
        return new StepBuilder("evaluationSlaveStep", jobRepository)
            .<FailedConditionDto, FailedConditionDto>chunk(10, transactionManager)
            .reader(conditionEvaluatingReader)
            .writer(failedConditionWriter)
            .build();
    }

    @Bean
    Step partitionedEvaluationStep(JobRepository jobRepository,
            Partitioner evaluationPartitioner,
            Step evaluationSlaveStep) {
        return new StepBuilder("partitionedEvaluationStep", jobRepository)
            .partitioner("evaluationSlaveStep", evaluationPartitioner)
            .step(evaluationSlaveStep)
            .build();
    }

    @Bean
    Job conditionEvaluationJob(JobRepository jobRepository,
            Step groupLoadingStep,
            Step partitionedEvaluationStep) {
        return new JobBuilder("conditionEvaluationJob", jobRepository)
            .start(groupLoadingStep)
            .next(partitionedEvaluationStep)
            .build();
    }

    // --- Row mapper ---

    private RowMapper<GroupElementDto> elementRowMapper() {
        return (rs, rowNum) -> new GroupElementDto(
            rs.getLong("id"),
            rs.getLong("group_id"),
            rs.getBigDecimal("element_value"),
            rs.getString("category"),
            rs.getObject("created_date", LocalDate.class));
    }
}
