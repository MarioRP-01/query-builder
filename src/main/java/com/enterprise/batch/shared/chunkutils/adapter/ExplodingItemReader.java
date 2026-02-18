package com.enterprise.batch.shared.chunkutils.adapter;

import com.enterprise.batch.shared.chunkutils.port.ItemExploder;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamReader;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;

/**
 * Decorator that absorbs a 1:N explosion between a delegate reader and
 * the chunk pipeline. For each item read from the delegate, the exploder
 * produces zero or more output items. Items that produce an empty
 * collection are silently skipped.
 *
 * @param <I> delegate item type (input)
 * @param <O> exploded item type (output)
 */
public class ExplodingItemReader<I, O> implements ItemStreamReader<O> {

    private final ItemReader<I> delegate;
    private final ItemExploder<I, O> exploder;
    private final ArrayDeque<O> buffer = new ArrayDeque<>();

    public ExplodingItemReader(ItemReader<I> delegate, ItemExploder<I, O> exploder) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.exploder = Objects.requireNonNull(exploder, "exploder");
    }

    @Override
    public O read() throws Exception {
        while (buffer.isEmpty()) {
            I item = delegate.read();
            if (item == null) {
                return null;
            }
            Collection<O> exploded = exploder.explode(item);
            if (exploded != null && !exploded.isEmpty()) {
                buffer.addAll(exploded);
            }
        }
        return buffer.poll();
    }

    @Override
    public void open(ExecutionContext executionContext) {
        if (delegate instanceof ItemStream stream) {
            stream.open(executionContext);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) {
        if (delegate instanceof ItemStream stream) {
            stream.update(executionContext);
        }
    }

    @Override
    public void close() {
        if (delegate instanceof ItemStream stream) {
            stream.close();
        }
    }
}
