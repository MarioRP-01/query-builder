package com.enterprise.batch.spring.adapter;

import com.enterprise.batch.spring.port.BatchQueryProvider;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Named registry for {@link BatchQueryProvider} instances.
 *
 * <p>In a Spring context, declare as a bean and register providers
 * by logical name:
 * <pre>{@code
 * @Bean
 * public QueryProviderRegistry queryProviderRegistry() {
 *     QueryProviderRegistry registry = new QueryProviderRegistry();
 *     registry.register("pendingOrders", pendingOrdersProvider());
 *     registry.register("overduePayments", overduePaymentsProvider());
 *     return registry;
 * }
 * }</pre>
 *
 * <p>Then inject the registry where needed to look up providers by name,
 * which is common in batch jobs with many readers.
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
