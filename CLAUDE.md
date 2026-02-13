# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

Maven + Java 17+. Oracle is the target database; Spring Batch is the runtime framework.

```bash
# Compile everything (requires Maven)
mvn compile

# Run gap tests (25 tests covering all 14 design gaps)
mvn compile exec:java -Dexec.mainClass="com.enterprise.batch.sql.AllGapsTest"

# Run edge-case tests (66 tests: nulls, injection, concurrency, etc.)
mvn compile exec:java -Dexec.mainClass="com.enterprise.batch.sql.EdgeCaseTests"

# Run Spring Batch integration tests (8 tests: provider, factory, registry)
mvn compile exec:java -Dexec.mainClass="com.enterprise.batch.sql.SpringBatchTests"

# Run Spring context test (6 tests: boot, beans, job execution)
mvn test-compile exec:java -Dexec.mainClass="com.enterprise.batch.sql.SpringContextTest"

# Run DML builder tests (33 tests: INSERT, UPDATE, DELETE, MERGE, Spring registry)
mvn test-compile exec:java -Dexec.mainClass="com.enterprise.batch.sql.DmlBuilderTests"

# Run order enrichment test (10 tests: read→process→DB+CSV write)
mvn test-compile exec:java -Dexec.mainClass="com.enterprise.batch.sql.OrderEnrichmentTest"
```

Tests are standalone (no JUnit). Each test class has `main()` and exits with code 1 on failure.

Core-only build (no Maven/Spring required — compiles SQL DSL + tests only):
```bash
find src -name "*.java" -not -path "*/spring/BatchReaderFactory.java" -not -path "*/spring/BatchWriterFactory.java" -not -path "*/spring/SpringBatchQueryConfig.java" | xargs javac -d out
java -cp out com.enterprise.batch.sql.AllGapsTest
java -cp out com.enterprise.batch.sql.EdgeCaseTests
java -cp out com.enterprise.batch.sql.SpringBatchTests
```

## Architecture

Type-safe SQL query DSL producing JDBC-ready SQL with named parameters for Oracle + Spring Batch.

### Entry points

1. **`SelectBuilder.query()`** — fluent SELECT: `.select()` → `.from()` → `.join()` → `.where()` → `.orderBy()` → `.build()` → `SqlResult`
2. **`InsertBuilder.insert()`** — fluent INSERT: `.into()` → `.set()`/`.columns().values()` → `.build()` / `.buildTemplate()`
3. **`UpdateBuilder.update()`** — fluent UPDATE: `.table()` → `.set()` → `.where()` → `.build()` (WHERE required; `.buildUnconditional()` for full-table)
4. **`DeleteBuilder.delete()`** — fluent DELETE: `.from()` → `.where()` → `.build()` (WHERE required; `.buildUnconditional()` for full-table)
5. **`MergeBuilder.merge()`** — Oracle MERGE: `.into()` → `.usingDual()`/`.usingSubquery()` → `.on()` → `.whenMatched*()`/`.whenNotMatchedInsert()` → `.build()`
6. **`Conditions.*`** (static import) — composable condition DSL: `eq()`, `or()`, `and()`, `eqIfPresent()`, `exists()`, etc.

### Spring Batch integration (`com.enterprise.batch.spring`)

**Read side:**
- **`BatchQueryProvider`** — `@FunctionalInterface` returning `SqlResult` from job params. One per query type, registered as a Spring bean.
- **`BatchReaderFactory`** — creates `JdbcCursorItemReader<T>` from a provider + `RowMapper` + `DataSource`. Converts named params to positional via `SqlResult.toPositional()`.
- **`QueryProviderRegistry`** — named lookup for providers when a job has many readers.

**Write side:**
- **`BatchDmlProvider`** — `@FunctionalInterface` returning `SqlResult` from job params. Same contract as `BatchQueryProvider` but for DML.
- **`BatchWriterFactory`** — creates `JdbcBatchItemWriter<T>` from a provider + `ItemSqlParameterSourceProvider` + `DataSource`. Uses named params from `buildTemplate()`.
- **`DmlProviderRegistry`** — named lookup for DML providers (same pattern as `QueryProviderRegistry`).

- **`SpringBatchQueryConfig`** — `@Configuration` that wires all four factories/registries as beans.

### Key design decisions

**Null semantics split**: Strict methods (`eq`, `gte`, ...) throw NPE on null value. Optional methods (`eqIfPresent`, `gteIfPresent`, `neqIfPresent`, `gtIfPresent`, `ltIfPresent`, `likeIfPresent`, `startsWithIfPresent`, `notInIfPresent`, ...) return `null`, which `.where()` silently filters out. This prevents accidental NULL comparisons in SQL.

**Table aliasing via reconstruction**: `Table.as(newAlias)` creates a **new instance** with fresh `Column` objects bound to the new alias. Columns must be assigned in the constructor, never inline — otherwise `as()` won't rebind them.

**Shared ParameterBinder for subqueries**: `SelectBuilder.query()` creates an isolated binder. `SelectBuilder.subquery(binder)` shares the parent's binder so parameter names (`:hint_N`) stay globally unique across CTEs, UNIONs, and correlated subqueries.

**Thread safety contract**: `BatchQueryProvider.buildQuery()` and `BatchDmlProvider.buildDml()` must create a fresh builder on every call. Never reuse builder instances.

**DML SET uses unqualified names**: `column.name()` in SET clauses (DML targets one table). WHERE reuses `Condition.toSql()` which calls `column.ref()` (qualified), matching Oracle's `UPDATE t alias SET col = :v WHERE alias.col = :v`.

**`buildTemplate()` for batch writing**: Produces `:column_name` placeholders (no `_N` suffix) with empty param map. `ItemSqlParameterSourceProvider` fills values per item at runtime.

**WHERE safety guard**: `UpdateBuilder.build()` and `DeleteBuilder.build()` require at least one WHERE condition. Use `buildUnconditional()` to explicitly opt into full-table DML.

**`ColumnValue<T>` record**: `MergeBuilder.ColumnValue<T>(Column<T>, T)` preserves type safety and insertion order for `usingDual()`.

### Generic bounds

Comparison methods use `Comparable<? super V>` (not `Comparable<V>`) to support types like `LocalDate` which implements `Comparable<ChronoLocalDate>`.

### Condition composition

All conditions implement `Condition.toSql(ParameterBinder)`. Composite conditions (`or()`, `and()`) recurse into children and wrap in parentheses. `orIfAny()`/`andIfAny()` filter null children and return null if none remain — enabling fully optional composite filters.

### SQL injection protection

`ExpressionValidator` checks all raw strings (identifiers, ON clauses, ORDER BY expressions) — blocks DML keywords (`DROP`, `DELETE`, `INSERT`, `UPDATE`, `ALTER`, `CREATE`, `TRUNCATE`, `EXEC`), comments (`--`, `/* */`), and semicolons. Values are always parameterized, never inlined into SQL.

### Dialect

Oracle 12c+ uses ANSI FETCH FIRST syntax. `Dialects.ORACLE` is an alias for `Dialects.ANSI` — single implementation, two names for semantic clarity. The `SqlDialect` interface is kept open for future database engines — implement the interface and pass via `.dialect(myDialect)`.

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

## Documentation

Full docs live in `docs/`. See `docs/README.md` for index. Key files:

- `architecture.md` — package structure, component diagrams, data flow
- `getting-started.md` — table definitions, first queries, result usage
- `select-builder.md` — SELECT API: JOINs, CTEs, UNIONs, subqueries, pagination
- `dml-builders.md` — INSERT, UPDATE, DELETE, MERGE reference
- `conditions.md` — strict vs optional, all condition types, composition, generics
- `spring-batch.md` — providers, reader/writer factories, registries, full job example
- `design-decisions.md` — 10 patterns, trade-offs, the 14 design gaps
- `security.md` — parameterization, expression validation, blocked patterns
- `unsupported.md` — features not yet covered, with workarounds
