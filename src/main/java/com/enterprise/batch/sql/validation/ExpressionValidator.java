package com.enterprise.batch.sql.validation;

import java.util.regex.Pattern;

/**
 * SQL injection guard for raw strings (identifiers, ON clauses, ORDER BY).
 * Blocks DML keywords, comments, and semicolons. Values are always parameterized.
 */
public final class ExpressionValidator {

    private ExpressionValidator() {}

    // Letters/underscore start, then alphanumeric/underscore/dot (for qualified refs)
    private static final Pattern IDENTIFIER_PATTERN =
            Pattern.compile("[a-zA-Z_][a-zA-Z0-9_.]*");

    // DML/DDL keywords that should never appear in expressions
    private static final Pattern DANGEROUS_KEYWORDS = Pattern.compile(
            "(?i)\\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|TRUNCATE|EXEC|EXECUTE|GRANT|REVOKE)\\b");

    // SQL comment markers
    private static final Pattern COMMENTS = Pattern.compile("(--|/\\*|\\*/)");

    /** Validates a simple identifier (table alias, CTE name, etc.) */
    public static void validateIdentifier(String identifier) {
        if (identifier == null || !IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid identifier: " + identifier);
        }
        if (DANGEROUS_KEYWORDS.matcher(identifier).find()) {
            throw new IllegalArgumentException(
                    "Dangerous keyword in identifier: " + identifier);
        }
    }

    /** Validates a SQL expression fragment (ON clause, ORDER BY, etc.) */
    public static void validateExpression(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("Expression cannot be null or blank");
        }
        if (DANGEROUS_KEYWORDS.matcher(expression).find()) {
            throw new IllegalArgumentException(
                    "Dangerous keyword in expression: " + expression);
        }
        if (COMMENTS.matcher(expression).find()) {
            throw new IllegalArgumentException(
                    "Comments not allowed in expression: " + expression);
        }
        if (expression.contains(";")) {
            throw new IllegalArgumentException(
                    "Semicolons not allowed in expression: " + expression);
        }
    }
}
