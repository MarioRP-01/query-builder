# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

Maven + Java 17+. Oracle is the target database; Spring Batch is the runtime framework.

```bash
# Compile everything
mvn compile

# Run all tests (187 tests across 8 classes)
mvn test

# Run a single test class
mvn test -Dtest=AllGapsTest
mvn test -Dtest=EdgeCaseTests
mvn test -Dtest=SpringContextTest
mvn test -Dtest=OrderEnrichmentTest
```

Tests use JUnit 5 + AssertJ (via `spring-boot-starter-test`).

## Architecture

Type-safe SQL query DSL producing JDBC-ready SQL with named parameters for Oracle + Spring Batch.

### Entry points

1. **`SelectBuilder.query()`** — fluent SELECT: `.select()` → `.from()` → `.join()` → `.where()` → `.orderBy()` → `.build()` → `SqlResult`
2. **`InsertBuilder.insert()`** — fluent INSERT: `.into()` → `.set()`/`.columns().values()` → `.build()` / `.buildTemplate()`
3. **`UpdateBuilder.update()`** — fluent UPDATE: `.table()` → `.set()` → `.where()` → `.build()` (WHERE required; `.buildUnconditional()` for full-table)
4. **`DeleteBuilder.delete()`** — fluent DELETE: `.from()` → `.where()` → `.build()` (WHERE required; `.buildUnconditional()` for full-table)
5. **`MergeBuilder.merge()`** — Oracle MERGE: `.into()` → `.usingDual()`/`.usingSubquery()` → `.on()` → `.whenMatched*()`/`.whenNotMatchedInsert()` → `.build()`
6. **`Conditions.*`** (static import) — composable condition DSL: `eq()`, `or()`, `and()`, `eqIfPresent()`, `exists()`, etc.
7. **`Cases.*`** (static import) — CASE expressions: `Cases.when(condition).then(val)...orElse(val).as(alias)` (searched), `Cases.of(column).when(val).then(val)...` (simple)

### Spring Batch integration (`com.enterprise.batch.shared.querybridge`) — shared framework

**`querybridge.port`** — pure Java contracts (zero Spring imports):
- **`BatchQueryProvider`** — `@FunctionalInterface` returning `SqlResult` from job params.
- **`BatchDmlProvider`** — `@FunctionalInterface` returning `SqlResult` from job params (DML variant).

**`querybridge.adapter`** — Spring Batch bridge:
- **`BatchReaderFactory`** — creates `JdbcCursorItemReader<T>` from a provider + `RowMapper` + `DataSource`. Converts named params to positional via `SqlResult.toPositional()`.
- **`BatchWriterFactory`** — creates `JdbcBatchItemWriter<T>` from a provider + `ItemSqlParameterSourceProvider` + `DataSource`. Uses named params from `buildTemplate()`.
- **`QueryProviderRegistry`** — named lookup for query providers when a job has many readers.
- **`DmlProviderRegistry`** — named lookup for DML providers (same pattern as `QueryProviderRegistry`).
- **`SpringBatchQueryConfig`** — `@Configuration` that wires all four factories/registries as beans.

### Vertical domain slices (`com.enterprise.batch.order`)

Each domain follows a 3-layer layout:
- **`domain/`** — pure Java: `Table` definitions, DTOs. Zero framework imports, only `sql.core.*`.
- **`application/`** — use cases: query providers (`OrderQueries`), business logic (`OrderEnricher`). Depends on domain + `sql.*` + `querybridge.port.*`, no Spring annotations.
- **`infrastructure/`** — Spring wiring: `@Configuration` / `@Bean` classes (`ProcessOrdersJobConfig`, `OrderEnrichmentJobConfig`). Depends on everything.

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
