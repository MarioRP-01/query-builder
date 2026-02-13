package com.enterprise.batch.sql.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlResult {

    private final String sql;
    private final Map<String, Object> parameters;

    public SqlResult(String sql, Map<String, Object> parameters) {
        this.sql = sql;
        this.parameters = parameters;
    }

    public String sql() { return sql; }

    public Map<String, Object> namedParameters() { return parameters; }

    /** Converts named parameters to positional (?) in declaration order. */
    public PositionalQuery toPositional() {
        String positionalSql = sql;
        List<Object> values = new ArrayList<>();
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            positionalSql = positionalSql.replace(":" + entry.getKey(), "?");
            values.add(entry.getValue());
        }
        return new PositionalQuery(positionalSql, values.toArray());
    }

    /** Returns the SQL with all parameter values inlined for debugging. */
    public String toDebugString() {
        String inlined = sql;
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            String replacement;
            if (entry.getValue() instanceof String) {
                replacement = "'" + entry.getValue() + "'";
            } else {
                replacement = String.valueOf(entry.getValue());
            }
            inlined = inlined.replace(":" + entry.getKey(), replacement);
        }
        return inlined;
    }

    /** Verifies every :param in the SQL has a matching entry in the map. */
    public void verify() {
        // Find all :param_N references in the SQL
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile(":(\\w+)").matcher(sql);
        while (m.find()) {
            String name = m.group(1);
            if (!parameters.containsKey(name)) {
                throw new IllegalStateException(
                        "SQL references :" + name + " but no parameter was bound");
            }
        }
    }

    public record PositionalQuery(String sql, Object[] values) {}
}
