package com.enterprise.batch.sql.param;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Objects;

/**
 * Converts typed Java values to safe Oracle SQL literals for inline use.
 * Values passed through this formatter appear directly in SQL text
 * (not as bind parameters).
 */
public final class SqlLiteralFormatter {

    private SqlLiteralFormatter() {}

    /**
     * Formats a Java value as an Oracle SQL literal.
     *
     * @throws NullPointerException     if value is null
     * @throws IllegalArgumentException if the type is not supported
     */
    public static String format(Object value) {
        Objects.requireNonNull(value, "literal value must not be null");

        if (value instanceof String s) {
            return "'" + s.replace("'", "''") + "'";
        }
        if (value instanceof BigDecimal bd) {
            return bd.toPlainString();
        }
        if (value instanceof Number n) {
            return n.toString();
        }
        if (value instanceof Boolean b) {
            return b ? "1" : "0";
        }
        if (value instanceof LocalDate ld) {
            return "DATE '" + ld + "'";
        }

        throw new IllegalArgumentException(
                "Unsupported literal type: " + value.getClass().getName());
    }
}
