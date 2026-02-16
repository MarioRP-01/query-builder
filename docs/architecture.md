# Architecture

## Package Structure

```
com.enterprise.batch
├── sql
│   ├── builder/        SelectBuilder, InsertBuilder, UpdateBuilder,
│   │                   DeleteBuilder, MergeBuilder, UnionBuilder, SqlResult
│   ├── condition/      Condition interface + 10 implementations + Conditions factory
│   ├── core/           Table, Column, SqlDialect, JoinType, SortDirection, ComparisonOp
│   ├── param/          ParameterBinder (named parameter management)
│   ├── validation/     ExpressionValidator, SchemaValidator
│   └── debug/          QueryDebugger
├── spring/             Shared framework (domain-agnostic)
│   ├── port/           BatchQueryProvider, BatchDmlProvider
│   └── adapter/        BatchReaderFactory, BatchWriterFactory,
│                       QueryProviderRegistry, DmlProviderRegistry,
│                       SpringBatchQueryConfig
└── order/              Vertical domain slice
    ├── domain/         OrderTable, CustomerTable, ProductTable, PaymentTable,
    │                   OrderDto, OrderDetailDto, EnrichedOrderDto
    ├── application/    OrderQueries, OrderEnricher
    └── infrastructure/ ProcessOrdersJobConfig, OrderEnrichmentJobConfig
```

## Component Diagram

```
┌─────────────────────────────────────────────────────────┐
│                     User Code                           │
│  Table definitions  │  Provider beans  │  Job config    │
└──────────┬──────────┴────────┬─────────┴───────┬────────┘
           │                   │                 │
           ▼                   ▼                 ▼
┌──────────────────┐  ┌────────────────┐  ┌──────────────┐
│   SQL Builders   │  │   Conditions   │  │  Spring Batch│
│  Select/Insert/  │◄─┤  eq, or, and,  │  │  Integration │
│  Update/Delete/  │  │  in, between,  │  │  Reader/     │
│  Merge/Union     │  │  exists, raw   │  │  Writer      │
└────────┬─────────┘  └───────┬────────┘  │  Factories   │
         │                    │           └──────┬───────┘
         ▼                    ▼                  │
  ┌──────────────┐    ┌──────────────┐           │
  │  SqlResult   │◄───┤ Parameter    │◄──────────┘
  │  (immutable) │    │ Binder       │
  └──────┬───────┘    └──────────────┘
         │
         ▼
  ┌──────────────┐
  │  JDBC / DB   │
  └──────────────┘
```

## Data Flow

### Read Path (SELECT → Spring Batch Reader)

```
1. BatchQueryProvider.buildQuery(jobParams)
      └─→ SelectBuilder.query().select().from().where().build()
              └─→ Conditions.eq/gte/in/... → Condition.toSql(binder)
              └─→ ParameterBinder.bind(value, hint) → ":hint_N"
      └─→ SqlResult { sql, namedParameters }

2. BatchReaderFactory.cursorReader(name, provider, mapper, params)
      └─→ result.verify()               // all :params bound?
      └─→ result.toPositional()          // :hint_N → ?
      └─→ JdbcCursorItemReader<T>        // positional SQL + Object[]
```

### Write Path (DML → Spring Batch Writer)

```
1. BatchDmlProvider.buildDml(jobParams)
      └─→ InsertBuilder.insert().into().set().buildTemplate()
              └─→ ":column_name" placeholders, empty param map
      └─→ SqlResult { templateSql, {} }

2. BatchWriterFactory.batchWriter(name, provider, paramSource, params)
      └─→ JdbcBatchItemWriter<T>         // named SQL
      └─→ ItemSqlParameterSourceProvider  // fills per-item values
```

## Core Abstractions

### Table + Column (Schema Layer)

`Table` is abstract. Each database table is a concrete subclass with typed `Column<T>` fields assigned in the constructor.

```
Table ──────────────────────────────────────────────
  │  tableName: String      ("orders")
  │  alias: String          ("o")
  │  columns: List<Column>  (populated via column())
  │
  ├─ column(name, type) → Column<T>
  ├─ as(newAlias) → Table   (abstract; reconstructs)
  ├─ declaration() → "orders o"
  └─ allColumns() → unmodifiable list

Column<T> ──────────────────────────────────────────
  │  name: String     ("id")
  │  type: Class<T>   (Long.class)
  │  table: Table     (backref)
  │
  ├─ ref() → "o.id"         (qualified)
  ├─ name() → "id"          (unqualified)
  ├─ refAs(alias) → "o.id AS alias"
  ├─ eqColumn(other) → "o.id = c.id"
  └─ countAs/sumAs/... → aggregate expressions
```

### ParameterBinder (Parameter Layer)

Generates globally unique named parameters using `hint + counter`:

```
bind("PENDING", "status") → ":status_1"   stored: {status_1 → "PENDING"}
bind(100, "amount")       → ":amount_2"   stored: {amount_2 → 100}
bind("PENDING", "status") → ":status_3"   stored: {status_3 → "PENDING"}
```

`AtomicInteger` counter ensures thread safety. `LinkedHashMap` preserves insertion order for positional conversion.

### SqlResult (Output Layer)

Immutable query output with multiple representations:

| Method | Returns | Use Case |
|--------|---------|----------|
| `sql()` | Named SQL (`:status_1`) | Debugging, logging |
| `namedParameters()` | `Map<String, Object>` | Spring NamedParameterJdbcTemplate |
| `toPositional()` | `PositionalQuery(sql, Object[])` | JDBC PreparedStatement |
| `toDebugString()` | SQL with inlined values | Development debugging |
| `verify()` | void (throws on mismatch) | Safety check |

## Dialect Abstraction

```java
public interface SqlDialect {
    String limit(int count);
    String offset(int skip);
    String limitOffset(int count, int skip);
}
```

| Dialect | LIMIT 10 | OFFSET 20 |
|---------|----------|-----------|
| `Dialects.ORACLE` | `FETCH FIRST 10 ROWS ONLY` | `OFFSET 20 ROWS` |
| `Dialects.ANSI` | `FETCH NEXT 10 ROWS ONLY` | `OFFSET 20 ROWS` |

Custom dialects: implement `SqlDialect` and pass via `.dialect(myDialect)`.
