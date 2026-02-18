package com.enterprise.batch.shared.filebridge;

import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.WritableResource;

/**
 * Factory that creates CSV {@link FlatFileItemWriter} instances using
 * {@link BeanWrapperFieldExtractor} for dynamic field extraction.
 *
 * <p>Field names drive both value extraction (via bean properties) and
 * header generation. Works with JavaBeans and Java records.
 *
 * <p>Typical usage:
 * <pre>{@code
 * @Bean
 * public FlatFileItemWriter<OrderDto> orderCsvWriter(CsvWriterFactory factory) {
 *     return factory.csvWriter("orderCsv",
 *             new FileSystemResource("output/orders.csv"),
 *             new String[]{"orderId", "customerName", "amount", "status"});
 * }
 * }</pre>
 *
 * <p>With custom header and delimiter:
 * <pre>{@code
 * factory.setDelimiter(";");
 * return factory.csvWriter("orderCsv",
 *         new FileSystemResource("output/orders.csv"),
 *         new String[]{"orderId", "customerName", "amount", "status"},
 *         w -> w.write("ID;NAME;AMOUNT;STATUS"));
 * }</pre>
 */
public class CsvWriterFactory {

    private String delimiter = ",";
    private String encoding = "UTF-8";
    private boolean shouldDeleteIfExists = true;

    /**
     * Creates a CSV writer with auto-generated header from field names.
     *
     * <p>Header line is produced by joining field names with the configured
     * delimiter. Bean properties are extracted in the declared order.
     *
     * @param <T>        item type (bean or record)
     * @param name       writer name (for restart data and logging)
     * @param resource   output file resource
     * @param fieldNames bean property names — drive both extraction and header
     * @return configured writer
     */
    public <T> FlatFileItemWriter<T> csvWriter(
            String name,
            WritableResource resource,
            String[] fieldNames) {

        return csvWriter(name, resource, fieldNames, defaultHeader(fieldNames));
    }

    /**
     * Creates a CSV writer with a custom header callback.
     *
     * @param <T>            item type (bean or record)
     * @param name           writer name (for restart data and logging)
     * @param resource       output file resource
     * @param fieldNames     bean property names for extraction
     * @param headerCallback writes the header line (or {@code null} to skip)
     * @return configured writer
     */
    public <T> FlatFileItemWriter<T> csvWriter(
            String name,
            WritableResource resource,
            String[] fieldNames,
            FlatFileHeaderCallback headerCallback) {

        if (fieldNames == null || fieldNames.length == 0) {
            throw new IllegalArgumentException("fieldNames must not be empty");
        }

        BeanWrapperFieldExtractor<T> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(fieldNames);

        DelimitedLineAggregator<T> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(delimiter);
        aggregator.setFieldExtractor(extractor);

        FlatFileItemWriter<T> writer = new FlatFileItemWriter<>();
        writer.setName(name);
        writer.setResource(resource);
        writer.setEncoding(encoding);
        writer.setShouldDeleteIfExists(shouldDeleteIfExists);
        writer.setLineAggregator(aggregator);
        if (headerCallback != null) {
            writer.setHeaderCallback(headerCallback);
        }
        return writer;
    }

    private FlatFileHeaderCallback defaultHeader(String[] fieldNames) {
        return w -> w.write(String.join(delimiter, fieldNames));
    }

    /** Field delimiter. Default {@code ","}. */
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    /** File encoding. Default {@code "UTF-8"}. */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /** Whether to delete existing file before writing. Default {@code true}. */
    public void setShouldDeleteIfExists(boolean shouldDeleteIfExists) {
        this.shouldDeleteIfExists = shouldDeleteIfExists;
    }
}
