package com.enterprise.batch.order.infrastructure;

import com.enterprise.batch.order.application.OrderAnalyticsDmlProviders;
import com.enterprise.batch.order.application.OrderAnalyticsProcessor;
import com.enterprise.batch.order.application.OrderAnalyticsQueries;
import com.enterprise.batch.order.domain.AnalyzedOrderDto;
import com.enterprise.batch.order.domain.OrderWindowDto;
import com.enterprise.batch.shared.filebridge.adapter.CsvWriterFactory;
import com.enterprise.batch.shared.querybridge.adapter.BatchReaderFactory;
import com.enterprise.batch.shared.querybridge.adapter.BatchWriterFactory;
import com.enterprise.batch.shared.querybridge.adapter.DmlProviderRegistry;
import com.enterprise.batch.shared.querybridge.adapter.QueryProviderRegistry;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Batch job: reads orders with window function analytics,
 * derives trend/velocity insights, writes to {@code order_analytics} + CSV.
 */
@Configuration
public class OrderAnalyticsJobConfig {

    @Autowired
    void registerProviders(QueryProviderRegistry queryRegistry,
                           DmlProviderRegistry dmlRegistry) {
        queryRegistry.register("orderAnalytics", OrderAnalyticsQueries.orderAnalytics());
        dmlRegistry.register("insertAnalytics", OrderAnalyticsDmlProviders.insertAnalytics());
    }

    // --- Reader ---

    @Bean
    JdbcCursorItemReader<OrderWindowDto> analyticsReader(
            BatchReaderFactory factory, QueryProviderRegistry registry) {
        return factory.cursorReader(
            "analyticsReader",
            registry.get("orderAnalytics"),
            analyticsRowMapper(),
            Map.of());
    }

    // --- Processor ---

    @Bean
    ItemProcessor<OrderWindowDto, AnalyzedOrderDto> analyticsProcessor() {
        return new OrderAnalyticsProcessor()::analyze;
    }

    // --- Writer 1: insert into order_analytics via BatchWriterFactory ---

    @Bean
    JdbcBatchItemWriter<AnalyzedOrderDto> analyticsDbWriter(
            BatchWriterFactory factory, DmlProviderRegistry registry) {
        return factory.templateWriter(
            "analyticsDbWriter",
            registry.get("insertAnalytics"),
            item -> new MapSqlParameterSource()
                .addValue("order_id", item.orderId())
                .addValue("amount", item.amount())
                .addValue("created_date", item.createdDate())
                .addValue("customer_name", item.customerName())
                .addValue("region", item.region())
                .addValue("tier", item.tier())
                .addValue("customer_order_seq", item.customerOrderSeq())
                .addValue("customer_running_total", item.customerRunningTotal())
                .addValue("prev_amount", item.prevAmount())
                .addValue("region_amount_rank", item.regionAmountRank())
                .addValue("region_spend_pct", item.regionSpendPct())
                .addValue("spend_quartile", item.spendQuartile())
                .addValue("trend", item.trend())
                .addValue("velocity_flag", item.velocityFlag()),
            Map.of());
    }

    // --- Writer 2: CSV report ---

    @Bean
    FlatFileItemWriter<AnalyzedOrderDto> analyticsCsvWriter(
            CsvWriterFactory csvFactory) throws IOException {
        Files.createDirectories(Path.of("output"));

        Map<String, String> columns = new LinkedHashMap<>();
        columns.put("orderId",              "order_id");
        columns.put("customerName",         "customer_name");
        columns.put("region",               "region");
        columns.put("amount",               "amount");
        columns.put("customerOrderSeq",     "customer_order_seq");
        columns.put("customerRunningTotal", "customer_running_total");
        columns.put("prevAmount",           "prev_amount");
        columns.put("regionAmountRank",     "region_amount_rank");
        columns.put("regionSpendPct",       "region_spend_pct");
        columns.put("spendQuartile",        "spend_quartile");
        columns.put("trend",                "trend");
        columns.put("velocityFlag",         "velocity_flag");

        return csvFactory.csvWriter("analyticsCsvWriter",
                new FileSystemResource("output/order_analytics.csv"),
                columns);
    }

    // --- Composite writer: DB + CSV ---

    @Bean
    CompositeItemWriter<AnalyzedOrderDto> analyticsCompositeWriter(
            JdbcBatchItemWriter<AnalyzedOrderDto> analyticsDbWriter,
            FlatFileItemWriter<AnalyzedOrderDto> analyticsCsvWriter) {
        CompositeItemWriter<AnalyzedOrderDto> writer = new CompositeItemWriter<>();
        writer.setDelegates(List.of(analyticsDbWriter, analyticsCsvWriter));
        return writer;
    }

    // --- Step + Job ---

    @Bean
    Step orderAnalyticsStep(JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcCursorItemReader<OrderWindowDto> analyticsReader,
            ItemProcessor<OrderWindowDto, AnalyzedOrderDto> analyticsProcessor,
            CompositeItemWriter<AnalyzedOrderDto> analyticsCompositeWriter) {
        return new StepBuilder("orderAnalyticsStep", jobRepository)
            .<OrderWindowDto, AnalyzedOrderDto>chunk(10, transactionManager)
            .reader(analyticsReader)
            .processor(analyticsProcessor)
            .writer(analyticsCompositeWriter)
            .build();
    }

    @Bean
    Job orderAnalyticsJob(JobRepository jobRepository, Step orderAnalyticsStep) {
        return new JobBuilder("orderAnalyticsJob", jobRepository)
            .start(orderAnalyticsStep)
            .build();
    }

    // --- Row mapper ---

    private RowMapper<OrderWindowDto> analyticsRowMapper() {
        return (rs, rowNum) -> new OrderWindowDto(
            rs.getLong("order_id"),
            rs.getBigDecimal("amount"),
            rs.getObject("created_date", LocalDate.class),
            rs.getString("customer_name"),
            rs.getString("region"),
            rs.getString("tier"),
            rs.getLong("customer_order_seq"),
            rs.getBigDecimal("customer_running_total"),
            rs.getBigDecimal("prev_amount"),
            rs.getLong("region_amount_rank"),
            rs.getBigDecimal("region_spend_pct"),
            rs.getLong("spend_quartile"));
    }
}
