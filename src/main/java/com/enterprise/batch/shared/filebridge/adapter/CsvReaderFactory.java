package com.enterprise.batch.shared.filebridge.adapter;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.Resource;

/**
 * Factory that creates CSV {@link FlatFileItemReader} instances using
 * {@link BeanWrapperFieldSetMapper} for dynamic field mapping.
 *
 * <p>Field names drive tokenization and bean property mapping. The first
 * line is skipped by default (header row).
 *
 * <p>Typical usage with a JavaBean:
 * <pre>{@code
 * @Bean
 * public FlatFileItemReader<OrderDto> orderCsvReader(CsvReaderFactory factory) {
 *     return factory.csvReader("orderCsv",
 *             new FileSystemResource("input/orders.csv"),
 *             new String[]{"orderId", "customerName", "amount", "status"},
 *             OrderDto.class);
 * }
 * }</pre>
 *
 * <p>With a custom {@link FieldSetMapper} and per-call delimiter:
 * <pre>{@code
 * return factory.csvReader("orderCsv",
 *         new FileSystemResource("input/orders.csv"),
 *         new String[]{"orderId", "customerName", "amount"},
 *         fs -> new OrderRecord(
 *                 fs.readLong("orderId"),
 *                 fs.readString("customerName"),
 *                 fs.readBigDecimal("amount")),
 *         ";");
 * }</pre>
 */
public class CsvReaderFactory {

    private String delimiter = ",";
    private String encoding = "UTF-8";
    private int linesToSkip = 1;

    /**
     * Creates a CSV reader with {@link BeanWrapperFieldSetMapper}.
     *
     * <p>Requires a JavaBean target with default constructor and setters.
     *
     * @param <T>        item type
     * @param name       reader name (for restart data and logging)
     * @param resource   input file resource
     * @param fieldNames column names — map to bean property names
     * @param targetType bean class to instantiate per row
     * @return configured reader
     */
    public <T> FlatFileItemReader<T> csvReader(
            String name,
            Resource resource,
            String[] fieldNames,
            Class<T> targetType) {

        BeanWrapperFieldSetMapper<T> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(targetType);

        return buildReader(name, resource, fieldNames, mapper, this.delimiter);
    }

    /**
     * Creates a CSV reader with {@link BeanWrapperFieldSetMapper} and
     * a per-call delimiter override.
     *
     * @param <T>        item type
     * @param name       reader name (for restart data and logging)
     * @param resource   input file resource
     * @param fieldNames column names — map to bean property names
     * @param targetType bean class to instantiate per row
     * @param delimiter  field delimiter for this reader
     * @return configured reader
     */
    public <T> FlatFileItemReader<T> csvReader(
            String name,
            Resource resource,
            String[] fieldNames,
            Class<T> targetType,
            String delimiter) {

        BeanWrapperFieldSetMapper<T> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(targetType);

        return buildReader(name, resource, fieldNames, mapper, delimiter);
    }

    /**
     * Creates a CSV reader with a custom {@link FieldSetMapper}.
     *
     * <p>Use this for Java records or types without default constructors.
     *
     * @param <T>        item type
     * @param name       reader name (for restart data and logging)
     * @param resource   input file resource
     * @param fieldNames column names — available via {@code fieldSet.readXxx(name)}
     * @param mapper     maps each tokenized line to a domain object
     * @return configured reader
     */
    public <T> FlatFileItemReader<T> csvReader(
            String name,
            Resource resource,
            String[] fieldNames,
            FieldSetMapper<T> mapper) {

        return buildReader(name, resource, fieldNames, mapper, this.delimiter);
    }

    /**
     * Creates a CSV reader with a custom {@link FieldSetMapper} and
     * a per-call delimiter override.
     *
     * @param <T>        item type
     * @param name       reader name (for restart data and logging)
     * @param resource   input file resource
     * @param fieldNames column names — available via {@code fieldSet.readXxx(name)}
     * @param mapper     maps each tokenized line to a domain object
     * @param delimiter  field delimiter for this reader
     * @return configured reader
     */
    public <T> FlatFileItemReader<T> csvReader(
            String name,
            Resource resource,
            String[] fieldNames,
            FieldSetMapper<T> mapper,
            String delimiter) {

        return buildReader(name, resource, fieldNames, mapper, delimiter);
    }

    // ===================== Internal builder =====================

    private <T> FlatFileItemReader<T> buildReader(
            String name,
            Resource resource,
            String[] fieldNames,
            FieldSetMapper<T> mapper,
            String effectiveDelimiter) {

        if (fieldNames == null || fieldNames.length == 0) {
            throw new IllegalArgumentException("fieldNames must not be empty");
        }

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(effectiveDelimiter);
        tokenizer.setNames(fieldNames);

        DefaultLineMapper<T> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mapper);

        FlatFileItemReader<T> reader = new FlatFileItemReader<>();
        reader.setName(name);
        reader.setResource(resource);
        reader.setEncoding(encoding);
        reader.setLinesToSkip(linesToSkip);
        reader.setLineMapper(lineMapper);
        return reader;
    }

    /** Field delimiter. Default {@code ","}. */
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    /** File encoding. Default {@code "UTF-8"}. */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /** Number of header lines to skip. Default {@code 1}. */
    public void setLinesToSkip(int linesToSkip) {
        this.linesToSkip = linesToSkip;
    }
}
