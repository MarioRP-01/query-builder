package com.enterprise.batch.sql;

import com.enterprise.batch.shared.filebridge.adapter.CsvReaderFactory;
import com.enterprise.batch.shared.filebridge.adapter.CsvWriterFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.core.io.FileSystemResource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link CsvWriterFactory} and {@link CsvReaderFactory}.
 */
class FileBridgeTests {

    // --- Test bean for BeanWrapper round-trips ---

    public static class ItemBean {
        private String name;
        private int quantity;
        private double price;

        public ItemBean() {}

        public ItemBean(String name, int quantity, double price) {
            this.name = name;
            this.quantity = quantity;
            this.price = price;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
        public double getPrice() { return price; }
        public void setPrice(double price) { this.price = price; }
    }

    // --- Test record for custom FieldSetMapper ---

    public record ItemRecord(String name, int quantity, double price) {}

    // ===================== CsvWriterFactory =====================

    @Test
    void writerAutoHeader(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("out.csv");
        CsvWriterFactory factory = new CsvWriterFactory();

        FlatFileItemWriter<ItemBean> writer = factory.csvWriter("test",
                new FileSystemResource(file),
                new String[]{"name", "quantity", "price"});

        writer.open(new ExecutionContext());
        writer.write(chunk(new ItemBean("Widget", 5, 9.99)));
        writer.close();

        List<String> lines = Files.readAllLines(file);
        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).isEqualTo("name,quantity,price");
        assertThat(lines.get(1)).isEqualTo("Widget,5,9.99");
    }

    @Test
    void writerCustomHeader(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("out.csv");
        CsvWriterFactory factory = new CsvWriterFactory();

        FlatFileItemWriter<ItemBean> writer = factory.csvWriter("test",
                new FileSystemResource(file),
                new String[]{"name", "price"},
                w -> w.write("PRODUCT_NAME,UNIT_PRICE"));

        writer.open(new ExecutionContext());
        writer.write(chunk(new ItemBean("Gadget", 3, 19.50)));
        writer.close();

        List<String> lines = Files.readAllLines(file);
        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).isEqualTo("PRODUCT_NAME,UNIT_PRICE");
        assertThat(lines.get(1)).isEqualTo("Gadget,19.5");
    }

    @Test
    void writerNullHeaderSkipsHeader(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("out.csv");
        CsvWriterFactory factory = new CsvWriterFactory();

        FlatFileItemWriter<ItemBean> writer = factory.csvWriter("test",
                new FileSystemResource(file),
                new String[]{"name"},
                null);

        writer.open(new ExecutionContext());
        writer.write(chunk(new ItemBean("X", 1, 1.0)));
        writer.close();

        List<String> lines = Files.readAllLines(file);
        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).isEqualTo("X");
    }

    @Test
    void writerSubsetOfFields(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("out.csv");
        CsvWriterFactory factory = new CsvWriterFactory();

        FlatFileItemWriter<ItemBean> writer = factory.csvWriter("test",
                new FileSystemResource(file),
                new String[]{"quantity", "name"});

        writer.open(new ExecutionContext());
        writer.write(chunk(new ItemBean("A", 10, 5.0)));
        writer.close();

        List<String> lines = Files.readAllLines(file);
        assertThat(lines.get(0)).isEqualTo("quantity,name");
        assertThat(lines.get(1)).isEqualTo("10,A");
    }

    @Test
    void writerCustomDelimiter(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("out.csv");
        CsvWriterFactory factory = new CsvWriterFactory();
        factory.setDelimiter(";");

        FlatFileItemWriter<ItemBean> writer = factory.csvWriter("test",
                new FileSystemResource(file),
                new String[]{"name", "price"});

        writer.open(new ExecutionContext());
        writer.write(chunk(new ItemBean("B", 2, 3.5)));
        writer.close();

        List<String> lines = Files.readAllLines(file);
        assertThat(lines.get(0)).isEqualTo("name;price");
        assertThat(lines.get(1)).isEqualTo("B;3.5");
    }

    @Test
    void writerMultipleItems(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("out.csv");
        CsvWriterFactory factory = new CsvWriterFactory();

        FlatFileItemWriter<ItemBean> writer = factory.csvWriter("test",
                new FileSystemResource(file),
                new String[]{"name", "quantity"});

        writer.open(new ExecutionContext());
        writer.write(chunk(
                new ItemBean("A", 1, 0),
                new ItemBean("B", 2, 0),
                new ItemBean("C", 3, 0)));
        writer.close();

        List<String> lines = Files.readAllLines(file);
        assertThat(lines).hasSize(4); // header + 3 rows
        assertThat(lines.get(1)).isEqualTo("A,1");
        assertThat(lines.get(3)).isEqualTo("C,3");
    }

    @Test
    void writerWithRecord(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("out.csv");
        CsvWriterFactory factory = new CsvWriterFactory();

        FlatFileItemWriter<ItemRecord> writer = factory.csvWriter("test",
                new FileSystemResource(file),
                new String[]{"name", "quantity", "price"});

        writer.open(new ExecutionContext());
        writer.write(chunk(new ItemRecord("RecItem", 7, 12.0)));
        writer.close();

        List<String> lines = Files.readAllLines(file);
        assertThat(lines.get(1)).isEqualTo("RecItem,7,12.0");
    }

    @Test
    void writerEmptyFieldNamesThrows() {
        CsvWriterFactory factory = new CsvWriterFactory();
        assertThatThrownBy(() -> factory.csvWriter("test",
                new FileSystemResource("dummy"), new String[]{}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fieldNames");
    }

    @Test
    void writerNullFieldNamesThrows() {
        CsvWriterFactory factory = new CsvWriterFactory();
        assertThatThrownBy(() -> factory.csvWriter("test",
                new FileSystemResource("dummy"), (String[]) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ===================== CsvReaderFactory =====================

    @Test
    void readerBeanWrapper(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("in.csv");
        Files.writeString(file, "name,quantity,price\nAlpha,10,4.5\nBeta,20,8.0\n");

        CsvReaderFactory factory = new CsvReaderFactory();
        FlatFileItemReader<ItemBean> reader = factory.csvReader("test",
                new FileSystemResource(file),
                new String[]{"name", "quantity", "price"},
                ItemBean.class);

        reader.open(new ExecutionContext());
        ItemBean first = reader.read();
        ItemBean second = reader.read();
        ItemBean eof = reader.read();
        reader.close();

        assertThat(first.getName()).isEqualTo("Alpha");
        assertThat(first.getQuantity()).isEqualTo(10);
        assertThat(first.getPrice()).isEqualTo(4.5);
        assertThat(second.getName()).isEqualTo("Beta");
        assertThat(eof).isNull();
    }

    @Test
    void readerCustomFieldSetMapper(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("in.csv");
        Files.writeString(file, "name,quantity,price\nGamma,3,7.25\n");

        CsvReaderFactory factory = new CsvReaderFactory();
        FlatFileItemReader<ItemRecord> reader = factory.csvReader("test",
                new FileSystemResource(file),
                new String[]{"name", "quantity", "price"},
                fs -> new ItemRecord(
                        fs.readString("name"),
                        fs.readInt("quantity"),
                        fs.readDouble("price")));

        reader.open(new ExecutionContext());
        ItemRecord item = reader.read();
        reader.close();

        assertThat(item).isEqualTo(new ItemRecord("Gamma", 3, 7.25));
    }

    @Test
    void readerCustomDelimiter(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("in.csv");
        Files.writeString(file, "name;quantity\nheader_skipped\nDelta;99\n");

        CsvReaderFactory factory = new CsvReaderFactory();
        factory.setDelimiter(";");
        factory.setLinesToSkip(2); // skip both lines before data

        FlatFileItemReader<ItemBean> reader = factory.csvReader("test",
                new FileSystemResource(file),
                new String[]{"name", "quantity"},
                ItemBean.class);

        reader.open(new ExecutionContext());
        ItemBean item = reader.read();
        reader.close();

        assertThat(item.getName()).isEqualTo("Delta");
        assertThat(item.getQuantity()).isEqualTo(99);
    }

    @Test
    void readerSubsetOfFields(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("in.csv");
        // CSV has 3 columns but we only map 2 (name, quantity)
        Files.writeString(file, "name,quantity\nEpsilon,42\n");

        CsvReaderFactory factory = new CsvReaderFactory();
        FlatFileItemReader<ItemBean> reader = factory.csvReader("test",
                new FileSystemResource(file),
                new String[]{"name", "quantity"},
                ItemBean.class);

        reader.open(new ExecutionContext());
        ItemBean item = reader.read();
        reader.close();

        assertThat(item.getName()).isEqualTo("Epsilon");
        assertThat(item.getQuantity()).isEqualTo(42);
        assertThat(item.getPrice()).isEqualTo(0.0); // unmapped, default
    }

    @Test
    void readerEmptyFieldNamesThrows() {
        CsvReaderFactory factory = new CsvReaderFactory();
        assertThatThrownBy(() -> factory.csvReader("test",
                new FileSystemResource("dummy"), new String[]{}, ItemBean.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fieldNames");
    }

    @Test
    void readerNullFieldNamesThrows() {
        CsvReaderFactory factory = new CsvReaderFactory();
        assertThatThrownBy(() -> factory.csvReader("test",
                new FileSystemResource("dummy"), null, ItemBean.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ===================== Round-trip: write then read =====================

    @Test
    void roundTripWriteThenRead(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("roundtrip.csv");
        String[] fields = new String[]{"name", "quantity", "price"};

        // Write
        CsvWriterFactory writerFactory = new CsvWriterFactory();
        FlatFileItemWriter<ItemBean> writer = writerFactory.csvWriter("w", new FileSystemResource(file), fields);
        writer.open(new ExecutionContext());
        writer.write(chunk(
                new ItemBean("X", 1, 10.0),
                new ItemBean("Y", 2, 20.5)));
        writer.close();

        // Read back
        CsvReaderFactory readerFactory = new CsvReaderFactory();
        FlatFileItemReader<ItemBean> reader = readerFactory.csvReader("r",
                new FileSystemResource(file), fields, ItemBean.class);
        reader.open(new ExecutionContext());
        ItemBean first = reader.read();
        ItemBean second = reader.read();
        ItemBean eof = reader.read();
        reader.close();

        assertThat(first.getName()).isEqualTo("X");
        assertThat(first.getQuantity()).isEqualTo(1);
        assertThat(first.getPrice()).isEqualTo(10.0);
        assertThat(second.getName()).isEqualTo("Y");
        assertThat(second.getQuantity()).isEqualTo(2);
        assertThat(second.getPrice()).isEqualTo(20.5);
        assertThat(eof).isNull();
    }

    // --- Helper ---

    @SafeVarargs
    private static <T> org.springframework.batch.item.Chunk<T> chunk(T... items) {
        return new org.springframework.batch.item.Chunk<>(List.of(items));
    }
}
