package com.enterprise.batch.shared.chunkutils.port;

import java.util.Collection;

@FunctionalInterface
public interface ItemExploder<I, O> {
    Collection<O> explode(I item);
}
