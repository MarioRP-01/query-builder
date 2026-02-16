package com.enterprise.batch.order.infrastructure;

import com.enterprise.batch.order.application.OrderDmlProviders;
import com.enterprise.batch.order.application.OrderEnricher;
import com.enterprise.batch.order.application.OrderQueries;
import com.enterprise.batch.order.domain.EnrichedOrderDto;
import com.enterprise.batch.order.domain.OrderDetailDto;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.util.Map;

/**
 * Reads pending orders, enriches them, and inserts summaries into
 * {@code order_summaries} using {@link com.enterprise.batch.sql.builder.InsertBuilder}
 * via the {@link BatchWriterFactory} bridge.
 */
@Configuration
public class SaveOrderSummariesJobConfig {

    @Autowired
    void registerProviders(QueryProviderRegistry queryRegistry,
                           DmlProviderRegistry dmlRegistry) {
        queryRegistry.register("orderDetails", OrderQueries.orderDetails());
        dmlRegistry.register("insertSummary", OrderDmlProviders.insertSummary());
    }

    // --- Reader ---

    @Bean
    JdbcCursorItemReader<OrderDetailDto> summaryOrderDetailReader(
            BatchReaderFactory factory, QueryProviderRegistry registry) {
        return factory.cursorReader(
            "summaryOrderDetailReader",
            registry.get("orderDetails"),
            orderDetailRowMapper(),
            Map.of("status", "PENDING"));
    }

    // --- Processor ---

    @Bean
    ItemProcessor<OrderDetailDto, EnrichedOrderDto> summaryEnrichmentProcessor() {
        return new OrderEnricher()::enrich;
    }

    // --- Writer: InsertBuilder via BatchWriterFactory ---

    @Bean
    JdbcBatchItemWriter<EnrichedOrderDto> summaryInsertWriter(
            BatchWriterFactory factory, DmlProviderRegistry registry) {
        return factory.templateWriter(
            "summaryInsertWriter",
            registry.get("insertSummary"),
            item -> new MapSqlParameterSource()
                .addValue("order_id", item.orderId())
                .addValue("customer_name", item.customerName())
                .addValue("customer_tier", item.customerTier())
                .addValue("product_name", item.productName())
                .addValue("original_amount", item.originalAmount())
                .addValue("tax_amount", item.taxAmount())
                .addValue("discount_amount", item.discountAmount())
                .addValue("final_amount", item.finalAmount())
                .addValue("priority", item.priority())
                .addValue("processed_date", item.processedDate()),
            Map.of());
    }

    // --- Step + Job ---

    @Bean
    Step saveOrderSummariesStep(JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcCursorItemReader<OrderDetailDto> summaryOrderDetailReader,
            ItemProcessor<OrderDetailDto, EnrichedOrderDto> summaryEnrichmentProcessor,
            JdbcBatchItemWriter<EnrichedOrderDto> summaryInsertWriter) {
        return new StepBuilder("saveOrderSummariesStep", jobRepository)
            .<OrderDetailDto, EnrichedOrderDto>chunk(10, transactionManager)
            .reader(summaryOrderDetailReader)
            .processor(summaryEnrichmentProcessor)
            .writer(summaryInsertWriter)
            .build();
    }

    @Bean
    Job saveOrderSummariesJob(JobRepository jobRepository,
                              Step saveOrderSummariesStep) {
        return new JobBuilder("saveOrderSummariesJob", jobRepository)
            .start(saveOrderSummariesStep)
            .build();
    }

    // --- Row mapper ---

    private RowMapper<OrderDetailDto> orderDetailRowMapper() {
        return (rs, rowNum) -> new OrderDetailDto(
            rs.getLong("id"),
            rs.getBigDecimal("amount"),
            rs.getString("status"),
            rs.getObject("created_date", LocalDate.class),
            rs.getString("customer_name"),
            rs.getString("customer_tier"),
            rs.getString("product_name"),
            rs.getString("product_category"));
    }
}
