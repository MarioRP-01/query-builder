package com.enterprise.batch.order.infrastructure;

import com.enterprise.batch.order.application.OrderQueries;
import com.enterprise.batch.order.domain.OrderDto;
import com.enterprise.batch.shared.querybridge.adapter.BatchReaderFactory;
import com.enterprise.batch.shared.querybridge.adapter.QueryProviderRegistry;

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

@Configuration
public class ProcessOrdersJobConfig {

    @Autowired
    void configureRegistry(QueryProviderRegistry registry) {
        registry.register("pendingOrders", OrderQueries.pendingOrders());
        registry.register("highValueCustomers", OrderQueries.highValueCustomers());
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
