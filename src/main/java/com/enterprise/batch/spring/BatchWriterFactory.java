package com.enterprise.batch.spring;

import com.enterprise.batch.sql.builder.SqlResult;

import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Objects;

/**
 * Factory that bridges the SQL DSL with Spring Batch writers.
 *
 * <p>Creates fully configured {@link JdbcBatchItemWriter} instances from
 * a {@link BatchDmlProvider} and Oracle {@link DataSource}. Uses named
 * parameters ({@code :col_name}) from {@code buildTemplate()}.
 *
 * <p>Typical usage:
 * <pre>{@code
 * @Bean
 * @StepScope
 * public JdbcBatchItemWriter<Order> orderWriter(
 *         BatchWriterFactory factory,
 *         @Value("#{jobParameters}") Map<String, Object> params) {
 *     return factory.batchWriter("orderWriter",
 *             updateOrderProvider(),
 *             new BeanPropertyItemSqlParameterSourceProvider<>(),
 *             params);
 * }
 * }</pre>
 */
public class BatchWriterFactory {

    private final DataSource dataSource;

    public BatchWriterFactory(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Creates a {@link JdbcBatchItemWriter} using named parameters from the provider.
     *
     * @param <T>                item type
     * @param name               writer name (for logging)
     * @param provider           DML provider — must be stateless
     * @param paramSourceProvider maps each item to named parameters
     * @param jobParams          job execution parameters forwarded to the provider
     * @return configured writer
     */
    public <T> JdbcBatchItemWriter<T> batchWriter(
            String name,
            BatchDmlProvider provider,
            ItemSqlParameterSourceProvider<T> paramSourceProvider,
            Map<String, Object> jobParams) {

        SqlResult result = resolveQuery(provider, jobParams);

        JdbcBatchItemWriter<T> writer = new JdbcBatchItemWriterBuilder<T>()
                .dataSource(dataSource)
                .sql(result.sql())
                .itemSqlParameterSourceProvider(paramSourceProvider)
                .build();
        return writer;
    }

    /**
     * Resolves the DML template without creating a writer.
     * Useful for logging, testing, and dry-run scenarios.
     */
    public SqlResult resolveQuery(BatchDmlProvider provider,
                                  Map<String, Object> jobParams) {
        return provider.buildDml(jobParams);
    }
}
