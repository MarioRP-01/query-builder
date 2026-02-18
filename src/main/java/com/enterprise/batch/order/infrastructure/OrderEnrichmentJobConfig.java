package com.enterprise.batch.order.infrastructure;

import com.enterprise.batch.order.application.OrderEnricher;
import com.enterprise.batch.order.application.OrderQueries;
import com.enterprise.batch.order.domain.EnrichedOrderDto;
import com.enterprise.batch.order.domain.OrderDetailDto;
import com.enterprise.batch.shared.filebridge.adapter.CsvWriterFactory;
import com.enterprise.batch.shared.querybridge.adapter.BatchReaderFactory;
import com.enterprise.batch.shared.querybridge.adapter.QueryProviderRegistry;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class OrderEnrichmentJobConfig {

    @Autowired
    void registerProviders(QueryProviderRegistry registry) {
        registry.register("orderDetails", OrderQueries.orderDetails());
    }

    // --- Reader ---

    @Bean
    JdbcCursorItemReader<OrderDetailDto> orderDetailReader(
            BatchReaderFactory factory, QueryProviderRegistry registry) {
        return factory.cursorReader(
            "orderDetailReader",
            registry.get("orderDetails"),
            orderDetailRowMapper(),
            Map.of("status", "PENDING"));
    }

    // --- Processor ---

    @Bean
    ItemProcessor<OrderDetailDto, EnrichedOrderDto> orderEnrichmentProcessor() {
        return new OrderEnricher()::enrich;
    }

    // --- Writer 1: insert enriched summaries into DB ---

    @Bean
    JdbcBatchItemWriter<EnrichedOrderDto> summaryDbWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<EnrichedOrderDto>()
            .dataSource(dataSource)
            .sql("INSERT INTO order_summaries "
                + "(order_id, customer_name, customer_tier, product_name, "
                + "original_amount, tax_amount, discount_amount, final_amount, "
                + "priority, processed_date) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .itemPreparedStatementSetter((item, ps) -> {
                ps.setLong(1, item.orderId());
                ps.setString(2, item.customerName());
                ps.setString(3, item.customerTier());
                ps.setString(4, item.productName());
                ps.setBigDecimal(5, item.originalAmount());
                ps.setBigDecimal(6, item.taxAmount());
                ps.setBigDecimal(7, item.discountAmount());
                ps.setBigDecimal(8, item.finalAmount());
                ps.setString(9, item.priority());
                ps.setObject(10, item.processedDate());
            })
            .build();
    }

    // --- Writer 2: CSV report ---

    @Bean
    FlatFileItemWriter<EnrichedOrderDto> summaryCsvWriter(
            CsvWriterFactory csvFactory) throws IOException {
        Files.createDirectories(Path.of("output"));

        Map<String, String> columns = new LinkedHashMap<>();
        columns.put("orderId",         "order_id");
        columns.put("customerName",    "customer_name");
        columns.put("customerTier",    "customer_tier");
        columns.put("productName",     "product_name");
        columns.put("originalAmount",  "original_amount");
        columns.put("taxAmount",       "tax_amount");
        columns.put("discountAmount",  "discount_amount");
        columns.put("finalAmount",     "final_amount");
        columns.put("priority",        "priority");
        columns.put("processedDate",   "processed_date");

        return csvFactory.csvWriter("summaryCsvWriter",
                new FileSystemResource("output/order_summary.csv"),
                columns);
    }

    // --- Composite writer: DB + CSV ---

    @Bean
    CompositeItemWriter<EnrichedOrderDto> enrichmentCompositeWriter(
            JdbcBatchItemWriter<EnrichedOrderDto> summaryDbWriter,
            FlatFileItemWriter<EnrichedOrderDto> summaryCsvWriter) {
        CompositeItemWriter<EnrichedOrderDto> writer = new CompositeItemWriter<>();
        writer.setDelegates(List.of(summaryDbWriter, summaryCsvWriter));
        return writer;
    }

    // --- Step + Job ---

    @Bean
    Step orderEnrichmentStep(JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcCursorItemReader<OrderDetailDto> orderDetailReader,
            ItemProcessor<OrderDetailDto, EnrichedOrderDto> orderEnrichmentProcessor,
            CompositeItemWriter<EnrichedOrderDto> enrichmentCompositeWriter) {
        return new StepBuilder("orderEnrichmentStep", jobRepository)
            .<OrderDetailDto, EnrichedOrderDto>chunk(10, transactionManager)
            .reader(orderDetailReader)
            .processor(orderEnrichmentProcessor)
            .writer(enrichmentCompositeWriter)
            .build();
    }

    @Bean
    Job orderEnrichmentJob(JobRepository jobRepository, Step orderEnrichmentStep) {
        return new JobBuilder("orderEnrichmentJob", jobRepository)
            .start(orderEnrichmentStep)
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
