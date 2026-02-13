package com.enterprise.batch.sql.param;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ParameterBinder {

    private final Map<String, Object> parameters = new LinkedHashMap<>();
    private final AtomicInteger counter = new AtomicInteger(1);

    /**
     * Binds a value and returns the named placeholder (e.g. ":status_1").
     * The hint is used to generate a descriptive parameter name.
     */
    public String bind(Object value, String hint) {
        String name = hint + "_" + counter.getAndIncrement();
        parameters.put(name, value);
        return ":" + name;
    }

    public Map<String, Object> getParameters() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(parameters));
    }
}
