package com.enterprise.batch.shared.filebridge.adapter;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for file-based Spring Batch readers and writers.
 *
 * <p>Provides {@link CsvReaderFactory} and {@link CsvWriterFactory} beans.
 * Import this configuration or let component scanning pick it up:
 * <pre>{@code
 * @Import(FileBridgeConfig.class)
 * @Configuration
 * public class MyBatchConfig { ... }
 * }</pre>
 *
 * <p>Override individual beans to customize (e.g. different delimiter):
 * <pre>{@code
 * @Bean
 * public CsvWriterFactory csvWriterFactory() {
 *     CsvWriterFactory factory = new CsvWriterFactory();
 *     factory.setDelimiter(";");
 *     return factory;
 * }
 * }</pre>
 */
@Configuration
public class FileBridgeConfig {

    @Bean
    public CsvReaderFactory csvReaderFactory() {
        return new CsvReaderFactory();
    }

    @Bean
    public CsvWriterFactory csvWriterFactory() {
        return new CsvWriterFactory();
    }
}
