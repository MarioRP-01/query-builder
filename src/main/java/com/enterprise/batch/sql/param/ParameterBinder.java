package com.enterprise.batch.sql.param;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Binds values to named parameters. {@link #bind(Object, String)} returns
 * {@code :hint_N} (counter starts at 1) and stores the value.
 * Thread-safe counter via {@link AtomicInteger}.
 */
public class ParameterBinder {

    private final Map<String, Object> parameters = new LinkedHashMap<>();
    private final AtomicInteger counter = new AtomicInteger(1);

    /**
     * Binds a value and returns the named placeholder (e.g. ":status_1").
     * The hint is used to generate a descriptive parameter name.
     */
    public String bind(Object value, String hint) {
        Object resolved = value instanceof Boolean b ? (b ? 1 : 0) : value;
        String name = hint + "_" + counter.getAndIncrement();
        parameters.put(name, resolved);
        return ":" + name;
    }

    public Map<String, Object> getParameters() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(parameters));
    }
}
