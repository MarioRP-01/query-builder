package com.enterprise.batch.example;

import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.core.SortDirection;
import com.enterprise.batch.spring.BatchQueryProvider;
import com.enterprise.batch.spring.BatchReaderFactory;
import com.enterprise.batch.spring.QueryProviderRegistry;

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
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static com.enterprise.batch.example.tables.CustomerTable.CUSTOMERS;
import static com.enterprise.batch.example.tables.OrderTable.ORDERS;
import static com.enterprise.batch.example.tables.ProductTable.PRODUCTS;
import static com.enterprise.batch.sql.condition.Conditions.*;

@Configuration
public class OrderEnrichmentJobConfig {

    @Autowired
    void registerProviders(QueryProviderRegistry registry) {
        registry.register("orderDetails", orderDetailsProvider());
    }

    // --- Query provider: 3-way join orders + customers + products ---

    BatchQueryProvider orderDetailsProvider() {
        return params -> {
            String status = (String) params.getOrDefault("status", "PENDING");
            return SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), ORDERS.STATUS.ref(),
                        ORDERS.CREATED_DATE.ref(),
                        CUSTOMERS.NAME.refAs("customer_name"),
                        CUSTOMERS.TIER.refAs("customer_tier"),
                        PRODUCTS.NAME.refAs("product_name"),
                        PRODUCTS.CATEGORY.refAs("product_category"))
                .from(ORDERS)
                .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
                .innerJoin(PRODUCTS, ORDERS.PRODUCT_ID, PRODUCTS.ID)
                .where(eq(ORDERS.STATUS, status))
                .orderBy(ORDERS.AMOUNT, SortDirection.DESC)
                .build();
        };
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

    // --- Processor: enrich with tax, discount, priority ---

    @Bean
    ItemProcessor<OrderDetailDto, EnrichedOrderDto> orderEnrichmentProcessor() {
        return item -> {
            BigDecimal tax = item.amount().multiply(new BigDecimal("0.10"));

            BigDecimal discountRate = switch (item.customerTier()) {
                case "GOLD"   -> new BigDecimal("0.15");
                case "SILVER" -> new BigDecimal("0.10");
                default       -> BigDecimal.ZERO;
            };
            BigDecimal discount = item.amount().multiply(discountRate);

            BigDecimal finalAmount = item.amount().add(tax).subtract(discount);

            String priority = item.amount().compareTo(new BigDecimal("1000")) >= 0
                ? "HIGH" : "NORMAL";

            return new EnrichedOrderDto(
                item.orderId(), item.customerName(), item.customerTier(),
                item.productName(), item.amount(), tax, discount, finalAmount,
                priority, LocalDate.now());
        };
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
    FlatFileItemWriter<EnrichedOrderDto> summaryCsvWriter() throws IOException {
        Files.createDirectories(Path.of("output"));

        FlatFileItemWriter<EnrichedOrderDto> writer = new FlatFileItemWriter<>();
        writer.setName("summaryCsvWriter");
        writer.setResource(new FileSystemResource("output/order_summary.csv"));
        writer.setHeaderCallback(w -> w.write(
            "order_id,customer_name,customer_tier,product_name,"
            + "original_amount,tax_amount,discount_amount,final_amount,"
            + "priority,processed_date"));
        writer.setLineAggregator(item -> String.join(",",
            String.valueOf(item.orderId()),
            item.customerName(),
            item.customerTier(),
            item.productName(),
            item.originalAmount().toPlainString(),
            item.taxAmount().toPlainString(),
            item.discountAmount().toPlainString(),
            item.finalAmount().toPlainString(),
            item.priority(),
            item.processedDate().toString()));
        return writer;
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
