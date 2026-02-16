# Type-Safe SQL Builder for Spring Batch — Complete Gap Fix Implementation

A lightweight, Hibernate-free, type-safe SQL query DSL that outputs clean JDBC-ready SQL
with named parameters. Designed for Spring Batch `JdbcCursorItemReader` but usable anywhere.

## Gap Fix Summary

| # | Gap | Severity | Solution |
|---|-----|----------|----------|
| 1 | **OR conditions** | Critical | Composable `Condition` interface with `and()`, `or()`, `andIfAny()`, `orIfAny()` |
| 2 | **Table alias collision** | Critical | `Table.as(String)` creates new instance with all columns re-bound to new alias |
| 3 | **Derived table joins** | Medium | `joinSubquery(type, subquery, alias, onClause)` |
| 4 | **UNION / UNION ALL** | Medium | `UnionBuilder` with shared `ParameterBinder` |
| 5 | **CTE (WITH clause)** | Low | `with(name, subquery)` on `SelectBuilder` |
| 6 | **Parameter ordering** | Critical | Named parameters (`:name`) eliminate positional bugs; `SqlResult.verify()` validates |
| 7 | **Named parameters** | Critical | `ParameterBinder` generates globally unique `:param_N` names with descriptive hints |
| 8 | **Null semantics** | Medium | `eq()` → strict (throws on null), `eqIfPresent()` → optional (returns null) |
| 9 | **Thread safety** | Medium | `query()` factory creates fresh builder each call; document in `BatchQueryProvider` |
| 10 | **Schema validation** | Medium | `SchemaValidator` checks Table definitions against `DatabaseMetaData` |
| 11 | **SQL injection** | High | `ExpressionValidator` validates all raw strings (blocks keywords, comments, quotes) |
| 12 | **Dialect portability** | Low | `SqlDialect` interface with Oracle, PostgreSQL, MySQL implementations |
| 13 | **Debug logging** | High | `SqlResult.toDebugString()` and `QueryDebugger.format()` |
| 14 | **Pagination** | Low | `limit()` / `offset()` with dialect-specific SQL generation |

## Architecture

```
com.enterprise.batch.sql
├── core/                  # Foundational types
│   ├── Table.java         # Abstract base with alias support (#2)
│   ├── Column.java        # Typed column with aggregates
│   ├── SqlDialect.java    # Dialect interface (#12)
│   ├── Dialects.java      # Oracle, PostgreSQL, MySQL
│   ├── SortDirection.java
│   ├── JoinType.java
│   └── ComparisonOp.java
├── condition/             # Composable condition system (#1, #8)
│   ├── Condition.java     # Core interface
│   ├── Conditions.java    # Static DSL factory (eq, or, and, eqIfPresent, etc.)
│   ├── SimpleCondition.java
│   ├── CompositeCondition.java   # AND/OR nesting
│   ├── BetweenCondition.java
│   ├── InListCondition.java
│   ├── NullCondition.java
│   ├── LikeCondition.java
│   ├── ColumnCondition.java      # Column-to-column (correlated subqueries)
│   ├── SubqueryCondition.java    # IN/NOT IN subquery
│   ├── ExistsCondition.java      # EXISTS/NOT EXISTS
│   └── RawCondition.java         # Validated raw SQL
├── builder/               # Query builders (#3, #4, #5, #14)
│   ├── SelectBuilder.java # Main entry point
│   ├── UnionBuilder.java  # UNION/UNION ALL
│   └── SqlResult.java     # Output with verification (#6, #13)
├── param/                 # Parameter management (#7)
│   └── ParameterBinder.java
├── validation/            # Safety (#10, #11)
│   ├── ExpressionValidator.java
│   └── SchemaValidator.java
└── debug/                 # Production debugging (#13)
    └── QueryDebugger.java

com.enterprise.batch.spring         # Spring Batch integration (shared framework)
├── port/
│   ├── BatchQueryProvider.java     # Provider contract (#9)
│   └── BatchDmlProvider.java       # DML provider contract
└── adapter/
    ├── BatchReaderFactory.java     # JdbcCursorItemReader factory
    ├── BatchWriterFactory.java     # JdbcBatchItemWriter factory
    ├── QueryProviderRegistry.java  # Named query lookup
    ├── DmlProviderRegistry.java    # Named DML lookup
    └── SpringBatchQueryConfig.java # @Configuration wiring

com.enterprise.batch.order          # Vertical domain slice
├── domain/                         # Pure Java: tables + DTOs
│   ├── OrderTable.java
│   ├── CustomerTable.java
│   ├── ProductTable.java
│   ├── PaymentTable.java
│   ├── OrderDto.java
│   ├── OrderDetailDto.java
│   └── EnrichedOrderDto.java
├── application/                    # Use cases: providers + business logic
│   ├── OrderQueries.java
│   └── OrderEnricher.java
└── infrastructure/                 # Spring @Configuration wiring
    ├── ProcessOrdersJobConfig.java
    └── OrderEnrichmentJobConfig.java
```

## Usage Examples

### Basic Query with Optional Filters

```java
import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.order.domain.OrderTable.ORDERS;
import static com.enterprise.batch.order.domain.CustomerTable.CUSTOMERS;

SqlResult result = SelectBuilder.query()
    .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), CUSTOMERS.NAME.ref())
    .from(ORDERS)
    .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
    .where(
        eqIfPresent(ORDERS.STATUS, params.get("status")),       // skipped if null
        gteIfPresent(ORDERS.AMOUNT, params.get("minAmount")),   // skipped if null
        eq(CUSTOMERS.REGION, "EU")                               // always applied
    )
    .orderBy(ORDERS.CREATED_DATE, SortDirection.DESC)
    .build();
```

### Complex OR Logic (Gap #1)

```java
.where(
    or(
        eq(ORDERS.STATUS, "PENDING"),
        and(
            gte(ORDERS.AMOUNT, new BigDecimal("1000")),
            eq(CUSTOMERS.TIER, "VIP")
        )
    )
)
// → WHERE (o.status = :status_1 OR (o.amount >= :amount_2 AND c.tier = :tier_3))
```

### Self-Join (Gap #2)

```java
OrderTable o2 = ORDERS.as("o2");  // New alias — all columns rebind

SqlResult result = SelectBuilder.query()
    .select(ORDERS.ID.ref(), o2.AMOUNT.refAs("other_amount"))
    .from(ORDERS)
    .innerJoin(o2, ORDERS.CUSTOMER_ID, o2.CUSTOMER_ID)
    .where(raw(ORDERS.ID.ref() + " <> " + o2.ID.ref()))
    .build();
// → FROM orders o INNER JOIN orders o2 ON o.customer_id = o2.customer_id
```

### Derived Table Join (Gap #3)

```java
ParameterBinder binder = new ParameterBinder();

SqlResult avgSub = SelectBuilder.subquery(binder)
    .select(subO.CUSTOMER_ID.ref(), subO.AMOUNT.avgAs("avg_amount"))
    .from(subO)
    .groupBy(subO.CUSTOMER_ID)
    .build();

SqlResult result = SelectBuilder.subquery(binder)
    .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref())
    .from(ORDERS)
    .joinSubquery(JoinType.INNER, avgSub, "summary",
            "summary.customer_id = " + ORDERS.CUSTOMER_ID.ref())
    .build();
```

### UNION ALL (Gap #4)

```java
ParameterBinder binder = new ParameterBinder();

SqlResult pending = SelectBuilder.subquery(binder)
    .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref())
    .from(ORDERS)
    .where(eq(ORDERS.STATUS, "PENDING"))
    .build();

SqlResult cancelled = SelectBuilder.subquery(binder)
    .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref())
    .from(ORDERS)
    .where(eq(ORDERS.STATUS, "CANCELLED"))
    .build();

SqlResult result = UnionBuilder.create(binder)
    .unionAll(pending)
    .unionAll(cancelled)
    .orderByExpr("o.amount", SortDirection.DESC)
    .build();
```

### CTE (Gap #5)

```java
ParameterBinder binder = new ParameterBinder();

SqlResult highValue = SelectBuilder.subquery(binder)
    .select(ORDERS.CUSTOMER_ID.ref())
    .from(ORDERS)
    .groupBy(ORDERS.CUSTOMER_ID)
    .havingRaw("SUM(o.amount) >= ?", new BigDecimal("10000"))
    .build();

SqlResult result = SelectBuilder.subquery(binder)
    .with("high_value", highValue)
    .select(CUSTOMERS.ID.ref(), CUSTOMERS.NAME.ref())
    .from(CUSTOMERS)
    .joinRaw(JoinType.INNER, "high_value hv", "hv.customer_id = c.id")
    .build();
// → WITH high_value AS (SELECT ...) SELECT c.id, c.name FROM customers c ...
```

### Null Semantics (Gap #8)

```java
// STRICT — throws NullPointerException if null
eq(ORDERS.STATUS, null);  // → NPE: "Use eqIfPresent() or isNull()"

// OPTIONAL — returns null (skipped by where())
eqIfPresent(ORDERS.STATUS, null);  // → null (not added to WHERE)

// EXPLICIT IS NULL
isNull(ORDERS.STATUS);  // → "o.status IS NULL"
```

### Debug Logging (Gap #13)

```java
SqlResult query = provider.buildQuery(params);
log.info("Executing:\n{}", QueryDebugger.format(query));

// Output:
// === SQL Query Debug ===
// SQL (named):
//   SELECT o.id FROM orders o WHERE o.status = :status_1
// SQL (positional):
//   SELECT o.id FROM orders o WHERE o.status = ?
// SQL (values inlined):
//   SELECT o.id FROM orders o WHERE o.status = 'PENDING'
// Parameters (1):
//   status_1 = PENDING (String)
// ======================
```

### Dialect Portability (Gap #12)

```java
SelectBuilder.query()
    .dialect(Dialects.POSTGRESQL)
    .select(ORDERS.ID.ref())
    .from(ORDERS)
    .limit(100)
    .offset(50)
    .build();
// PostgreSQL → LIMIT 100 OFFSET 50
// Oracle     → OFFSET 50 ROWS FETCH NEXT 100 ROWS ONLY
```

### Schema Validation (Gap #10)

```java
@Test
void schemaShouldMatchDefinitions() {
    SchemaValidator validator = new SchemaValidator(dataSource);
    List<String> errors = validator.validate(
        OrderTable.ORDERS,
        CustomerTable.CUSTOMERS,
        PaymentTable.PAYMENTS
    );
    assertThat(errors).isEmpty();
}
```

### Spring Batch Integration

```java
@Bean
@StepScope
public JdbcCursorItemReader<Order> reader(
        BatchReaderFactory factory,
        OrderQueryProvider provider,
        @Value("#{jobParameters}") Map<String, Object> params) {

    return factory.create("orderReader", provider,
        new BeanPropertyRowMapper<>(Order.class), params);
}
```

## Defining New Tables

```java
public final class InvoiceTable extends Table {
    public static final InvoiceTable INVOICES = new InvoiceTable("inv");

    public final Column<Long>       ID;
    public final Column<String>     INVOICE_NUMBER;
    public final Column<BigDecimal> TOTAL;
    public final Column<LocalDate>  DUE_DATE;

    public InvoiceTable(String alias) {
        super("invoices", alias);
        this.ID             = column("id", Long.class);
        this.INVOICE_NUMBER = column("invoice_number", String.class);
        this.TOTAL          = column("total", BigDecimal.class);
        this.DUE_DATE       = column("due_date", LocalDate.class);
    }

    @Override
    public InvoiceTable as(String newAlias) {
        return new InvoiceTable(newAlias);
    }
}
```

**Key rule:** Column fields must be assigned in the constructor (not inline),
so that `as(String)` creates fresh columns bound to the new alias.

## Thread Safety (Gap #9)

`BatchQueryProvider.buildQuery()` must create a fresh `SelectBuilder` on every call.
Add this architectural test:

```java
@Test
void providersShouldNotShareState() {
    var r1 = provider.buildQuery(Map.of("status", "A"));
    var r2 = provider.buildQuery(Map.of("status", "B"));
    assertThat(r1.toDebugString()).contains("'A'");
    assertThat(r2.toDebugString()).contains("'B'");
    assertThat(r1.toDebugString()).doesNotContain("'B'");
}
```

## Dependencies

The core SQL library (`sql.*` packages) has **zero external dependencies** — pure Java 21.

Only the Spring integration layer (`spring.*`) requires:
- `spring-batch-infrastructure` (JdbcCursorItemReader)
- `spring-jdbc` (RowMapper)
- `slf4j-api` (logging)
