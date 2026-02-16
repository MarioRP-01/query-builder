package com.enterprise.batch.shared.querybridge.adapter;

import com.enterprise.batch.shared.querybridge.port.BatchDmlProvider;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Named registry for {@link BatchDmlProvider} instances.
 *
 * <p>Same pattern as {@link QueryProviderRegistry} but for DML providers.
 *
 * <pre>{@code
 * @Bean
 * public DmlProviderRegistry dmlProviderRegistry() {
 *     DmlProviderRegistry registry = new DmlProviderRegistry();
 *     registry.register("updateStatus", updateStatusProvider());
 *     registry.register("softDelete", softDeleteProvider());
 *     return registry;
 * }
 * }</pre>
 */
public class DmlProviderRegistry {

    private final Map<String, BatchDmlProvider> providers = new LinkedHashMap<>();

    public void register(String name, BatchDmlProvider provider) {
        providers.put(name, provider);
    }

    public BatchDmlProvider get(String name) {
        BatchDmlProvider provider = providers.get(name);
        if (provider == null) {
            throw new IllegalArgumentException("No DML provider registered: " + name);
        }
        return provider;
    }

    public Map<String, BatchDmlProvider> all() {
        return Collections.unmodifiableMap(providers);
    }
}
