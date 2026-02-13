# Security

## Defense Layers

The DSL uses two complementary layers to prevent SQL injection:

```
Layer 1: Parameterization    — values are NEVER inlined into SQL
Layer 2: Expression validation — raw identifiers/expressions are blocklisted
```

### Layer 1: Parameterization

All user-supplied **values** go through `ParameterBinder.bind()`, which produces named placeholders:

```java
eq(ORDERS.STATUS, userInput)
// SQL:    o.status = :status_1
// Params: {status_1 → userInput}
// userInput is NEVER concatenated into the SQL string
```

This applies to all condition types: comparisons, IN lists, BETWEEN bounds, LIKE patterns, raw conditions, and DML SET values.

### Layer 2: ExpressionValidator

Raw **identifiers and expressions** (table names, column names, ON clauses, ORDER BY expressions) are validated before inclusion in SQL:

```java
ExpressionValidator.validateIdentifier(name)     // strict: [a-zA-Z_][a-zA-Z0-9_]*
ExpressionValidator.validateExpression(expr)      // permissive but blocks dangerous patterns
```

#### Blocked Patterns

| Pattern | Example | Reason |
|---------|---------|--------|
| DML keywords | `DROP`, `DELETE`, `INSERT`, `UPDATE`, `ALTER`, `CREATE`, `TRUNCATE` | Prevents DDL/DML injection |
| Execution keywords | `EXEC`, `EXECUTE`, `GRANT`, `REVOKE` | Prevents privilege escalation |
| Line comments | `--` | Prevents comment-based injection |
| Block comments | `/* ... */` | Same |
| Statement terminator | `;` | Prevents multi-statement injection |

#### Protected Entry Points

Every method that accepts raw SQL strings validates them:

| Method | Validation |
|--------|------------|
| `SelectBuilder.with(cteName, ...)` | `validateIdentifier(cteName)` |
| `SelectBuilder.joinRaw(type, name, on)` | `validateExpression(name)`, `validateExpression(on)` |
| `SelectBuilder.selectRaw(expr)` | `validateExpression(expr)` |
| `SelectBuilder.groupByExpr(expr)` | `validateExpression(expr)` |
| `SelectBuilder.orderByExpr(expr, dir)` | `validateExpression(expr)` |
| `SelectBuilder.havingRaw(expr, value)` | `validateExpression(expr)` |
| `Conditions.raw(sql, values)` | `validateExpression(sql)` |

### What's NOT Validated

- Table names and column names defined in `Table` subclasses — these are compile-time constants in your source code, not user input.
- The `SqlDialect` output (FETCH FIRST, OFFSET) — dialect implementations are trusted code.

## SchemaValidator

Optional runtime validation against live database metadata:

```java
SchemaValidator validator = new SchemaValidator(dataSource);
List<String> errors = validator.validate(ORDERS, CUSTOMERS, PAYMENTS);
// ["Column 'orders.xyz' not found in database"]
```

Checks:
- Table exists in database
- All defined columns exist in the table

Use at application startup or in integration tests to catch schema drift.

## Thread Safety

Providers must create fresh builders per call. If a builder were shared across threads, one thread's conditions could leak into another's query — a correctness bug, not a security one, but worth preventing.

```java
// SAFE: fresh builder, isolated ParameterBinder
return params -> SelectBuilder.query().select(...).build();

// UNSAFE: shared mutable state
SelectBuilder shared = SelectBuilder.query();
return params -> shared.where(eq(ORDERS.STATUS, params.get("s"))).build();
```

## Recommendations

1. **Never concatenate user input into raw expressions** — use parameterized conditions instead.
2. **Use strict conditions** (`eq`, `gt`) for required parameters. The NPE on null is intentional — it catches bugs where a required parameter is missing.
3. **Validate at the boundary** — the DSL protects the SQL layer, but validate user input (types, ranges, allowed values) before it reaches the query builder.
4. **Run `result.verify()`** in development/test to catch parameter binding mismatches.
5. **Use `SchemaValidator`** in CI to catch column renames or table drops before deployment.
