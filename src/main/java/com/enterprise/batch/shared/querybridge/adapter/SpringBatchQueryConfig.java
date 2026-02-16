package com.enterprise.batch.shared.querybridge.adapter;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Auto-configuration for the SQL DSL / Spring Batch integration.
 *
 * <p>Provides a {@link BatchReaderFactory} and {@link QueryProviderRegistry}
 * bean. Import this configuration or let component scanning pick it up:
 * <pre>{@code
 * @Import(SpringBatchQueryConfig.class)
 * @Configuration
 * public class MyBatchConfig { ... }
 * }</pre>
 *
 * <p>Override individual beans to customize (e.g. different fetch size):
 * <pre>{@code
 * @Bean
 * public BatchReaderFactory batchReaderFactory(DataSource ds) {
 *     BatchReaderFactory factory = new BatchReaderFactory(ds);
 *     factory.setFetchSize(5000);
 *     factory.setQueryTimeout(300);
 *     return factory;
 * }
 * }</pre>
 */
@Configuration
public class SpringBatchQueryConfig {

    @Bean
    public BatchReaderFactory batchReaderFactory(DataSource dataSource) {
        return new BatchReaderFactory(dataSource);
    }

    @Bean
    public QueryProviderRegistry queryProviderRegistry() {
        return new QueryProviderRegistry();
    }

    @Bean
    public BatchWriterFactory batchWriterFactory(DataSource dataSource) {
        return new BatchWriterFactory(dataSource);
    }

    @Bean
    public DmlProviderRegistry dmlProviderRegistry() {
        return new DmlProviderRegistry();
    }
}
