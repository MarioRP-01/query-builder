package com.enterprise.batch.sql.debug;

import com.enterprise.batch.sql.builder.SqlResult;

import java.util.Map;

/**
 * Debug utility: formats an {@link SqlResult} showing named-param SQL,
 * positional SQL, values-inlined SQL, and parameter list with types.
 */
public final class QueryDebugger {

    private QueryDebugger() {}

    public static String format(SqlResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("=== SQL Query Debug ===\n");

        sb.append("SQL (named):\n  ").append(result.sql()).append("\n");

        SqlResult.PositionalQuery pq = result.toPositional();
        sb.append("SQL (positional):\n  ").append(pq.sql()).append("\n");

        sb.append("SQL (values inlined):\n  ").append(result.toDebugString()).append("\n");

        Map<String, Object> params = result.namedParameters();
        sb.append("Parameters (").append(params.size()).append("):\n");
        for (Map.Entry<String, Object> e : params.entrySet()) {
            Object val = e.getValue();
            String typeName = val != null ? val.getClass().getSimpleName() : "null";
            sb.append("  ").append(e.getKey()).append(" = ").append(val)
                    .append(" (").append(typeName).append(")\n");
        }
        sb.append("======================");
        return sb.toString();
    }
}
