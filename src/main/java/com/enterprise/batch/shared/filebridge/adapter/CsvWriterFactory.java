package com.enterprise.batch.shared.filebridge.adapter;

import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.WritableResource;

import java.util.Map;

/**
 * Factory that creates CSV {@link FlatFileItemWriter} instances using
 * {@link BeanWrapperFieldExtractor} for dynamic field extraction.
 *
 * <p>Field names drive both value extraction (via bean properties) and
 * header generation. Works with JavaBeans and Java records.
 *
 * <p>Typical usage with a map (keys = bean properties, values = CSV headers):
 * <pre>{@code
 * @Bean
 * public FlatFileItemWriter<OrderDto> orderCsvWriter(CsvWriterFactory factory) {
 *     return factory.csvWriter("orderCsv",
 *             new FileSystemResource("output/orders.csv"),
 *             Map.of("orderId", "order_id",
 *                    "customerName", "customer_name",
 *                    "amount", "amount"));
 * }
 * }</pre>
 *
 * <p>With per-call delimiter:
 * <pre>{@code
 * return factory.csvWriter("orderCsv",
 *         new FileSystemResource("output/orders.csv"),
 *         columns, ";");
 * }</pre>
 *
 * <p>With field-name array and custom header callback:
 * <pre>{@code
 * return factory.csvWriter("orderCsv",
 *         new FileSystemResource("output/orders.csv"),
 *         new String[]{"orderId", "customerName", "amount"},
 *         w -> w.write("ID;NAME;AMOUNT"));
 * }</pre>
 */
public class CsvWriterFactory {

    private String delimiter = ",";
    private String encoding = "UTF-8";
    private boolean shouldDeleteIfExists = true;

    // ===================== Map-based API =====================

    /**
     * Creates a CSV writer with header derived from a column map.
     *
     * <p>Map keys are bean property names (drive extraction), values are
     * CSV header names. Insertion order determines column order — use
     * {@link java.util.LinkedHashMap} or {@link Map#of} for deterministic ordering.
     *
     * @param <T>      item type (bean or record)
     * @param name     writer name (for restart data and logging)
     * @param resource output file resource
     * @param columns  bean property → CSV header name (insertion-ordered)
     * @return configured writer
     */
    public <T> FlatFileItemWriter<T> csvWriter(
            String name,
            WritableResource resource,
            Map<String, String> columns) {

        return csvWriter(name, resource, columns, this.delimiter);
    }

    /**
     * Creates a CSV writer with header derived from a column map and
     * a per-call delimiter override.
     *
     * @param <T>       item type (bean or record)
     * @param name      writer name (for restart data and logging)
     * @param resource  output file resource
     * @param columns   bean property → CSV header name (insertion-ordered)
     * @param delimiter field delimiter for this writer
     * @return configured writer
     */
    public <T> FlatFileItemWriter<T> csvWriter(
            String name,
            WritableResource resource,
            Map<String, String> columns,
            String delimiter) {

        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("columns must not be empty");
        }
        String[] fieldNames = columns.keySet().toArray(String[]::new);
        String[] headerNames = columns.values().toArray(String[]::new);
        FlatFileHeaderCallback header = w -> w.write(String.join(delimiter, headerNames));
        return buildWriter(name, resource, fieldNames, header, delimiter);
    }

    // ===================== String[] API =====================

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

        return csvWriter(name, resource, fieldNames,
                w -> w.write(String.join(this.delimiter, fieldNames)));
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

        return buildWriter(name, resource, fieldNames, headerCallback, this.delimiter);
    }

    // ===================== Internal builder =====================

    private <T> FlatFileItemWriter<T> buildWriter(
            String name,
            WritableResource resource,
            String[] fieldNames,
            FlatFileHeaderCallback headerCallback,
            String effectiveDelimiter) {

        if (fieldNames == null || fieldNames.length == 0) {
            throw new IllegalArgumentException("fieldNames must not be empty");
        }

        BeanWrapperFieldExtractor<T> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(fieldNames);

        DelimitedLineAggregator<T> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(effectiveDelimiter);
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
