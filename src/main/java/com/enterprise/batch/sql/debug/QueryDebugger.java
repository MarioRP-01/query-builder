package com.enterprise.batch.sql.debug;

import com.enterprise.batch.sql.builder.SqlResult;

import java.util.Map;

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
            sb.append("  ").append(e.getKey()).append(" = ").append(e.getValue())
                    .append(" (").append(e.getValue().getClass().getSimpleName()).append(")\n");
        }
        sb.append("======================");
        return sb.toString();
    }
}
