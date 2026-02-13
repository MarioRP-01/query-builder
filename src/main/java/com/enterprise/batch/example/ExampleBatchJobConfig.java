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
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.util.Map;

import static com.enterprise.batch.example.tables.CustomerTable.CUSTOMERS;
import static com.enterprise.batch.example.tables.OrderTable.ORDERS;
import static com.enterprise.batch.sql.condition.Conditions.*;

@Configuration
public class ExampleBatchJobConfig {

    @Autowired
    void configureRegistry(QueryProviderRegistry registry) {
        registry.register("pendingOrders", pendingOrdersProvider());
        registry.register("highValueCustomers", highValueCustomersProvider());
    }

    BatchQueryProvider pendingOrdersProvider() {
        return params -> {
            String status = (String) params.getOrDefault("status", "PENDING");
            return SelectBuilder.query()
                .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), ORDERS.STATUS.ref(),
                        ORDERS.CREATED_DATE.ref(), CUSTOMERS.NAME.refAs("customer_name"))
                .from(ORDERS)
                .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
                .where(eq(ORDERS.STATUS, status))
                .orderBy(ORDERS.CREATED_DATE, SortDirection.DESC)
                .build();
        };
    }

    BatchQueryProvider highValueCustomersProvider() {
        return params -> SelectBuilder.query()
            .select(CUSTOMERS.ID.ref(), CUSTOMERS.NAME.ref(), CUSTOMERS.TIER.ref())
            .from(CUSTOMERS)
            .where(eq(CUSTOMERS.TIER, "GOLD"))
            .build();
    }

    @Bean
    public JdbcCursorItemReader<OrderDto> orderReader(
            BatchReaderFactory factory, QueryProviderRegistry registry) {
        return factory.cursorReader(
            "orderReader",
            registry.get("pendingOrders"),
            orderRowMapper(),
            Map.of("status", "PENDING"));
    }

    @Bean
    public Step processOrdersStep(JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JdbcCursorItemReader<OrderDto> orderReader) {
        return new StepBuilder("processOrdersStep", jobRepository)
            .<OrderDto, OrderDto>chunk(10, transactionManager)
            .reader(orderReader)
            .writer(chunk -> chunk.getItems().forEach(
                order -> System.out.println("  >> " + order)))
            .build();
    }

    @Bean
    public Job processOrdersJob(JobRepository jobRepository, Step processOrdersStep) {
        return new JobBuilder("processOrdersJob", jobRepository)
            .start(processOrdersStep)
            .build();
    }

    private RowMapper<OrderDto> orderRowMapper() {
        return (rs, rowNum) -> new OrderDto(
            rs.getLong("id"),
            rs.getBigDecimal("amount"),
            rs.getString("status"),
            rs.getObject("created_date", LocalDate.class),
            rs.getString("customer_name"));
    }
}
