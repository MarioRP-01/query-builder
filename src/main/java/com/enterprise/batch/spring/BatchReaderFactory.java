package com.enterprise.batch.spring;

import com.enterprise.batch.sql.builder.SqlResult;

import java.util.Map;

/**
 * Factory for creating JDBC cursor item readers from BatchQueryProviders.
 * In a real Spring Batch setup this would return JdbcCursorItemReader.
 * This is a framework-free stub — Spring dependencies are optional.
 */
public class BatchReaderFactory {

    /**
     * Creates a reader configuration from the given provider and parameters.
     * Returns the SqlResult that would be fed to JdbcCursorItemReader.
     */
    public SqlResult createQuery(String readerName,
                                  BatchQueryProvider provider,
                                  Map<String, Object> params) {
        SqlResult result = provider.buildQuery(params);
        result.verify();
        return result;
    }
}
