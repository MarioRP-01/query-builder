package com.enterprise.batch.spring;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple registry for BatchQueryProviders.
 * In a Spring context, providers would be auto-discovered via component scanning.
 */
public class QueryProviderRegistry {

    private final Map<String, BatchQueryProvider> providers = new LinkedHashMap<>();

    public void register(String name, BatchQueryProvider provider) {
        providers.put(name, provider);
    }

    public BatchQueryProvider get(String name) {
        BatchQueryProvider provider = providers.get(name);
        if (provider == null) {
            throw new IllegalArgumentException("No provider registered: " + name);
        }
        return provider;
    }

    public Map<String, BatchQueryProvider> all() {
        return Collections.unmodifiableMap(providers);
    }
}
