package com.enterprise.batch.sql;

import com.enterprise.batch.shared.chunkutils.adapter.ExplodingItemReader;

import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamReader;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.*;

class ExplodingItemReaderTest {

    @Test
    void explodesAndSkipsEmptyCollections() throws Exception {
        // delegate returns 3 items then null
        ItemReader<String> delegate = sequenceReader("A", "B", "C");

        ExplodingItemReader<String, String> reader = new ExplodingItemReader<>(delegate,
            item -> switch (item) {
                case "A" -> List.of("A1", "A2");   // 1st → 2 results
                case "B" -> List.of();              // 2nd → 0 (skip)
                case "C" -> List.of("C1");          // 3rd → 1 result
                default -> List.of();
            });

        assertThat(reader.read()).isEqualTo("A1");
        assertThat(reader.read()).isEqualTo("A2");
        assertThat(reader.read()).isEqualTo("C1");
        assertThat(reader.read()).isNull();
    }

    @Test
    void delegatesLifecycleToItemStream() throws Exception {
        AtomicBoolean opened = new AtomicBoolean();
        AtomicBoolean closed = new AtomicBoolean();

        ItemStreamReader<String> delegate = new ItemStreamReader<>() {
            @Override public String read() { return null; }
            @Override public void open(ExecutionContext ec)   { opened.set(true); }
            @Override public void update(ExecutionContext ec)  { /* no-op */ }
            @Override public void close()                      { closed.set(true); }
        };

        ExplodingItemReader<String, String> reader = new ExplodingItemReader<>(delegate, List::of);

        reader.open(new ExecutionContext());
        assertThat(opened).isTrue();

        reader.close();
        assertThat(closed).isTrue();
    }

    @Test
    void skipsLifecycleForNonStreamDelegate() {
        ItemReader<String> delegate = () -> null;

        ExplodingItemReader<String, String> reader = new ExplodingItemReader<>(delegate, List::of);

        // should not throw — gracefully ignores lifecycle
        assertThatNoException().isThrownBy(() -> {
            reader.open(new ExecutionContext());
            reader.update(new ExecutionContext());
            reader.close();
        });
    }

    @SafeVarargs
    private static <T> ItemReader<T> sequenceReader(T... items) {
        int[] idx = {0};
        return () -> idx[0] < items.length ? items[idx[0]++] : null;
    }
}
