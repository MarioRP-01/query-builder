# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

No Maven/Gradle — pure `javac`. Java 17+.

```bash
# Compile everything
find src -name "*.java" | xargs javac -d out

# Run gap tests (26 tests covering all 14 design gaps)
java -cp out com.enterprise.batch.sql.AllGapsTest

# Run edge-case tests (70 tests: nulls, injection, concurrency, etc.)
java -cp out com.enterprise.batch.sql.EdgeCaseTests
```

Tests are standalone (no JUnit). Each test class has `main()` and exits with code 1 on failure.

## Architecture

Type-safe SQL query DSL producing JDBC-ready SQL with named parameters. Zero external dependencies in core; Spring Batch integration layer is optional.

### Two entry points

1. **`SelectBuilder.query()`** — fluent builder: `.select()` → `.from()` → `.join()` → `.where()` → `.orderBy()` → `.build()` → `SqlResult`
2. **`Conditions.*`** (static import) — composable condition DSL: `eq()`, `or()`, `and()`, `eqIfPresent()`, `exists()`, etc.

### Key design decisions

**Null semantics split**: Strict methods (`eq`, `gte`, ...) throw NPE on null value. Optional methods (`eqIfPresent`, `gteIfPresent`, ...) return `null`, which `.where()` silently filters out. This prevents accidental NULL comparisons in SQL.

**Table aliasing via reconstruction**: `Table.as(newAlias)` creates a **new instance** with fresh `Column` objects bound to the new alias. Columns must be assigned in the constructor, never inline — otherwise `as()` won't rebind them.

**Shared ParameterBinder for subqueries**: `SelectBuilder.query()` creates an isolated binder. `SelectBuilder.subquery(binder)` shares the parent's binder so parameter names (`:hint_N`) stay globally unique across CTEs, UNIONs, and correlated subqueries.

**Thread safety contract**: `BatchQueryProvider.buildQuery()` must create a fresh `SelectBuilder` on every call. Never reuse builder instances.

### Generic bounds

Comparison methods use `Comparable<? super V>` (not `Comparable<V>`) to support types like `LocalDate` which implements `Comparable<ChronoLocalDate>`.

### Condition composition

All conditions implement `Condition.toSql(ParameterBinder)`. Composite conditions (`or()`, `and()`) recurse into children and wrap in parentheses. `orIfAny()`/`andIfAny()` filter null children and return null if none remain — enabling fully optional composite filters.

### SQL injection protection

`ExpressionValidator` checks all raw strings (identifiers, ON clauses, ORDER BY expressions) — blocks DML keywords (`DROP`, `DELETE`, `INSERT`, `UPDATE`, `ALTER`, `CREATE`, `TRUNCATE`, `EXEC`), comments (`--`, `/* */`), and semicolons. Values are always parameterized, never inlined into SQL.

### Dialect portability

`SqlDialect` interface with ANSI (default), Oracle, PostgreSQL, MySQL implementations. Only affects LIMIT/OFFSET syntax. Set via `.dialect(Dialects.POSTGRESQL)`.

## Adding a new table

```java
public final class FooTable extends Table {
    public static final FooTable FOO = new FooTable("f");
    public final Column<Long> ID;

    public FooTable(String alias) {
        super("foo", alias);
        this.ID = column("id", Long.class); // must be in constructor
    }

    @Override
    public FooTable as(String newAlias) { return new FooTable(newAlias); }
}
```

## Adding a new condition type

Implement `Condition`, call `binder.bind(value, hint)` for each parameter in `toSql()`, add a static factory method in `Conditions`.
