package com.enterprise.batch.spring;

import com.enterprise.batch.sql.builder.SqlResult;

import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Objects;

/**
 * Factory that bridges the SQL DSL with Spring Batch readers.
 *
 * <p>Creates fully configured {@link JdbcCursorItemReader} instances from
 * a {@link BatchQueryProvider} and Oracle {@link DataSource}. The DSL's
 * named parameters are converted to positional {@code ?} placeholders
 * via {@link SqlResult#toPositional()}.
 *
 * <p>Typical usage in a {@code @Configuration} class:
 * <pre>{@code
 * @Bean
 * public BatchReaderFactory readerFactory(DataSource oracleDs) {
 *     return new BatchReaderFactory(oracleDs);
 * }
 *
 * @Bean
 * @StepScope
 * public JdbcCursorItemReader<Order> orderReader(
 *         BatchReaderFactory factory,
 *         @Value("#{jobParameters['status']}") String status) {
 *     return factory.cursorReader("orderReader", pendingOrdersProvider(),
 *             orderRowMapper(), Map.of("status", status));
 * }
 * }</pre>
 */
public class BatchReaderFactory {

    private final DataSource dataSource;
    private int fetchSize = 1000;
    private int queryTimeout = 0;

    public BatchReaderFactory(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Creates a {@link JdbcCursorItemReader} wired with the query from the provider.
     *
     * <p>The provider builds a fresh query on each call. The resulting SQL is
     * verified, converted to positional parameters, and set on the reader with
     * an {@link ArgumentPreparedStatementSetter}.
     *
     * @param <T>       row type
     * @param name      reader name (used for restart data and logging)
     * @param provider  query provider — must be stateless
     * @param rowMapper maps each ResultSet row to a domain object
     * @param jobParams job execution parameters forwarded to the provider
     * @return fully configured reader — Spring calls afterPropertiesSet() automatically
     *         in managed Steps
     */
    public <T> JdbcCursorItemReader<T> cursorReader(
            String name,
            BatchQueryProvider provider,
            RowMapper<T> rowMapper,
            Map<String, Object> jobParams) {

        SqlResult result = resolveQuery(provider, jobParams);
        SqlResult.PositionalQuery pq = result.toPositional();

        JdbcCursorItemReader<T> reader = new JdbcCursorItemReader<>();
        reader.setName(name);
        reader.setDataSource(dataSource);
        reader.setSql(pq.sql());
        reader.setRowMapper(rowMapper);
        reader.setFetchSize(fetchSize);
        if (queryTimeout > 0) {
            reader.setQueryTimeout(queryTimeout);
        }
        reader.setPreparedStatementSetter(
                new ArgumentPreparedStatementSetter(pq.values()));
        return reader;
    }

    /**
     * Resolves and verifies the query without creating a reader.
     * Useful for logging, testing, and dry-run scenarios.
     */
    public SqlResult resolveQuery(BatchQueryProvider provider,
                                  Map<String, Object> jobParams) {
        SqlResult result = provider.buildQuery(jobParams);
        result.verify();
        return result;
    }

    /** Oracle fetch size hint. Default 1000. */
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    /** Query timeout in seconds. 0 = no timeout (default). */
    public void setQueryTimeout(int seconds) {
        this.queryTimeout = seconds;
    }
}
