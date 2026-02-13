# Design Decisions & Patterns

## Patterns

### 1. Fluent Builder

Every SQL construct uses the builder pattern with method chaining. Builders are **mutable** and **single-use** — construct, chain, call `.build()`, discard.

```java
SelectBuilder.query()              // create
    .select(...).from(...).where(...)   // chain
    .build();                      // consume → SqlResult (immutable)
```

Why not immutable builders (copy-on-write)? Simpler implementation, lower allocation overhead, and the intended usage pattern (fresh per call) makes immutability unnecessary.

### 2. Condition Composition (Composite Pattern)

`Condition` is a single interface. `CompositeCondition` holds a list of children + operator (AND/OR). Conditions nest arbitrarily to any depth:

```
CompositeCondition(OR)
├── CompositeCondition(AND)
│   ├── SimpleCondition(status = :v)
│   └── SimpleCondition(amount >= :v)
└── SimpleCondition(region = :v)
```

Each condition is self-contained — it knows how to render itself to SQL via `toSql(binder)`. No central visitor or switch statement needed.

### 3. Table Reconstruction for Aliasing

`Table.as(newAlias)` creates a **new instance** with fresh `Column` objects bound to the new alias. This enables self-joins without alias collision:

```java
OrderTable o1 = ORDERS;           // columns: o.id, o.amount
OrderTable o2 = ORDERS.as("o2");  // columns: o2.id, o2.amount
// o1.ID.ref() → "o.id"
// o2.ID.ref() → "o2.id"
```

The constraint: columns **must** be assigned in the constructor (not inline field initializers), because `as()` calls the constructor. If columns were inline-initialized, they'd be created before the constructor body runs and wouldn't pick up the new alias.

### 4. Null Semantics Split

Two families of methods prevent accidental NULL comparisons:

| Family | Null behavior | When to use |
|--------|---------------|-------------|
| Strict (`eq`, `gt`) | Throws NPE | Required params — null is a bug |
| Optional (`eqIfPresent`) | Returns `null` | Search filters — null means "no filter" |

`.where()` silently drops `null` conditions. This lets you build dynamic queries without if/else chains:

```java
// Instead of:
List<Condition> conds = new ArrayList<>();
if (status != null) conds.add(eq(ORDERS.STATUS, status));
if (minAmount != null) conds.add(gte(ORDERS.AMOUNT, minAmount));

// You write:
.where(
    eqIfPresent(ORDERS.STATUS, status),
    gteIfPresent(ORDERS.AMOUNT, minAmount)
)
```

### 5. Shared ParameterBinder for Subqueries

A top-level query gets its own `ParameterBinder`. Subqueries, CTEs, and UNIONs share the parent's binder so parameter names stay globally unique:

```
Main query binder (counter: 1)
├── CTE "recent": bind("PENDING", "status") → :status_1  (counter: 2)
├── Main SELECT:   bind(100, "amount")      → :amount_2  (counter: 3)
└── Subquery:      bind("EU", "region")     → :region_3  (counter: 4)
```

Without sharing, two subqueries might both produce `:status_1`, causing parameter collisions.

### 6. Template Mode for Batch Writing

`buildTemplate()` produces SQL with `:column_name` placeholders (no `_N` suffix) and an empty parameter map. Spring Batch's `ItemSqlParameterSourceProvider` fills values per item at runtime.

```
build()         → "... WHERE o.id = :id_1"    {id_1 → 1001}     (one-shot)
buildTemplate() → "... WHERE o.id = :id"      {}                 (per-item)
```

The placeholder names match bean property names, enabling automatic mapping.

### 7. WHERE Safety Guard

`UpdateBuilder.build()` and `DeleteBuilder.build()` **require** at least one WHERE condition. This prevents accidental full-table mutations:

```java
// Compile succeeds but throws at runtime:
UpdateBuilder.update().table(T).set(C, v).build();  // IllegalStateException

// Explicit opt-in:
UpdateBuilder.update().table(T).set(C, v).buildUnconditional();
```

SELECT and INSERT don't need this guard — SELECT without WHERE is common, and INSERT targets specific rows by definition.

### 8. Sealed Interface for SET Clauses

UpdateBuilder uses a sealed interface for type-safe SET variants:

```java
sealed interface SetClause {
    Column<?> column();
    String toSql(ParameterBinder binder);
}
record ValueSetClause(Column<?> column, Object value) implements SetClause { ... }
record NullSetClause(Column<?> column) implements SetClause { ... }
record SubquerySetClause(Column<?> column, SqlResult subquery) implements SetClause { ... }
```

Each variant knows how to render itself. No instanceof checks or casts needed — pattern matching on sealed types is exhaustive.

### 9. Functional Provider Interface

`BatchQueryProvider` and `BatchDmlProvider` are `@FunctionalInterface` — they can be lambdas:

```java
@Bean
public BatchQueryProvider provider() {
    return params -> SelectBuilder.query()...build();  // lambda
}
```

This keeps provider definitions concise and co-located with their Spring bean registration.

### 10. Registry Pattern

`QueryProviderRegistry` and `DmlProviderRegistry` provide named lookup for jobs with many readers/writers. Simple `Map<String, Provider>` with validation on `get()`.

---

## Key Trade-offs

### Oracle-First vs Multi-Dialect

The DSL targets Oracle. Multi-row INSERT uses `INSERT ALL ... SELECT 1 FROM DUAL`. MERGE uses Oracle-specific syntax (`WHEN MATCHED DELETE`, `USING DUAL`). The `SqlDialect` interface exists for future expansion, but only `ORACLE` and `ANSI` are implemented. Pragmatic choice: Oracle is the deployment target; abstracting prematurely would add complexity without value.

### Named Parameters vs Positional

The DSL generates named parameters (`:status_1`) internally for clarity and composability. Spring Batch readers need positional (`?`), so `SqlResult.toPositional()` converts at the boundary. Named params are easier to debug and merge across subqueries.

### No JUnit

Tests are standalone `main()` methods that exit with code 1 on failure. This avoids the JUnit dependency and makes tests runnable with just `javac` + `java`. Trade-off: no test runner features (parallel execution, reporting, IDE integration).

### Mutable Builders

Builders accumulate state (clauses, conditions, parameters) during construction. They're not thread-safe and should be discarded after `.build()`. The alternative — immutable copy-on-write builders — would add allocation overhead for no practical benefit, since the intended pattern is always fresh-per-call.

### ExpressionValidator Blocklist

Raw SQL expressions are validated against a blocklist of dangerous keywords. This is defense-in-depth (values are already parameterized), but it catches accidental SQL injection in raw expressions like `orderByExpr()` or `joinRaw()`. The blocklist approach has false positives (e.g., a column named "updated_at" won't trigger, but "UPDATE" as a standalone token would).

---

## The 14 Design Gaps

This DSL was designed to address 14 specific gaps found in typical query builders:

| # | Gap | Solution |
|---|-----|----------|
| 1 | OR conditions hard to compose | `CompositeCondition` with AND/OR nesting |
| 2 | Table alias collisions in self-joins | `Table.as()` reconstructs with fresh columns |
| 3 | Can't join to derived tables | `joinSubquery()` with parameterized subqueries |
| 4 | No UNION support | `UnionBuilder` composes queries |
| 5 | No CTE (WITH) support | `SelectBuilder.with()` clause |
| 6 | No parameter verification | `SqlResult.verify()` checks all `:params` bound |
| 7 | Positional-only parameters | Named params (`:hint_N`) throughout |
| 8 | Unsafe null handling | Strict/optional split with null filtering |
| 9 | Thread-unsafe builders | Fresh-per-call contract via providers |
| 10 | No type safety | Generic `Column<T>` + typed conditions |
| 11 | SQL injection in raw expressions | `ExpressionValidator` blocklist |
| 12 | Single-dialect lock-in | `SqlDialect` interface |
| 13 | Poor debug output | `toDebugString()` + `QueryDebugger` |
| 14 | No pagination | `limit()` + `offset()` (dialect-aware) |
