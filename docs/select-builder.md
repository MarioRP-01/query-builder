# SELECT Builder Reference

## Entry Points

| Method | ParameterBinder | Use Case |
|--------|-----------------|----------|
| `SelectBuilder.query()` | Fresh (isolated) | Standalone queries |
| `SelectBuilder.subquery(binder)` | Shared | CTEs, UNIONs, correlated subqueries |

## Fluent API

### SELECT Clause

```java
.select("o.id", "o.amount")             // column refs
.select(ORDERS.ID.ref())                // type-safe ref
.select(ORDERS.ID.refAs("order_id"))    // aliased: o.id AS order_id
.select(ORDERS.AMOUNT.sumAs("total"))   // SUM(o.amount) AS total
.selectDistinct(ORDERS.STATUS.ref())    // SELECT DISTINCT
.selectRaw("NVL(o.amount, 0)")          // raw expression (validated)
```

### FROM Clause

```java
.from(ORDERS)                           // FROM orders o
.fromSubquery(subResult, "sub")         // FROM (SELECT ...) sub
```

### JOIN Clause

```java
// Column-based (simple equi-join)
.innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
.leftJoin(PRODUCTS, ORDERS.PRODUCT_ID, PRODUCTS.ID)
.join(JoinType.RIGHT, PAYMENTS, ORDERS.ID, PAYMENTS.ORDER_ID)

// Multi-condition ON
.join(JoinType.INNER, CUSTOMERS,
    eqColumn(ORDERS.CUSTOMER_ID, CUSTOMERS.ID),
    eq(CUSTOMERS.TIER, "GOLD"))

// Subquery join
.joinSubquery(JoinType.INNER, subResult, "recent", "recent.id = o.id")

// CTE / raw join
.joinRaw(JoinType.INNER, "recent_orders r", "r.id = o.id")
```

**JoinType values**: `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`

### WHERE Clause

```java
.where(
    eq(ORDERS.STATUS, "PENDING"),
    gte(ORDERS.AMOUNT, 100),
    eqIfPresent(ORDERS.REGION, region)    // null → skipped
)
// Null conditions silently filtered. No WHERE clause if all are null.
```

See [Conditions](conditions.md) for the full predicate DSL.

### GROUP BY / HAVING

```java
.groupBy(ORDERS.STATUS, ORDERS.REGION)
.groupByExpr("TRUNC(o.created_date)")       // raw (validated)
.having(gt(ORDERS.AMOUNT, 1000))            // typed condition
.havingRaw("SUM(o.amount) > ?", 10000)      // raw (validated)
```

### ORDER BY

```java
.orderBy(ORDERS.CREATED_DATE, SortDirection.DESC)
.orderByExpr("NVL(o.amount, 0)", SortDirection.ASC)   // raw (validated)
```

### Pagination

```java
.limit(50)            // FETCH FIRST 50 ROWS ONLY (Oracle)
.offset(100)          // OFFSET 100 ROWS
.dialect(Dialects.ANSI)  // switch to ANSI syntax
```

### Build

```java
SqlResult result = builder.build();       // final query
SqlResult sub = builder.buildSubquery();  // same, for clarity in subquery context
```

---

## Common Techniques

### Self-JOIN

```java
OrderTable o1 = ORDERS;
OrderTable o2 = ORDERS.as("o2");

SelectBuilder.query()
    .select(o1.ID.ref(), o2.ID.ref())
    .from(o1)
    .innerJoin(o2, o1.CUSTOMER_ID, o2.CUSTOMER_ID)
    .where(neq(o1.ID, o2.ID))
    .build();
```

### CTE (WITH Clause)

```java
ParameterBinder shared = new ParameterBinder();

SqlResult cte = SelectBuilder.subquery(shared)
    .select(ORDERS.CUSTOMER_ID.ref(), ORDERS.AMOUNT.sumAs("total"))
    .from(ORDERS)
    .groupBy(ORDERS.CUSTOMER_ID)
    .build();

SqlResult main = SelectBuilder.subquery(shared)
    .with("totals", cte)
    .select("t.customer_id", "t.total", CUSTOMERS.NAME.ref())
    .from(CUSTOMERS)
    .joinRaw(JoinType.INNER, "totals t", "t.customer_id = c.id")
    .where(gt(ORDERS.AMOUNT, 1000))
    .build();
```

### UNION / UNION ALL

```java
ParameterBinder shared = new ParameterBinder();

SqlResult q1 = SelectBuilder.subquery(shared)
    .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref())
    .from(ORDERS)
    .where(eq(ORDERS.STATUS, "PENDING"))
    .build();

SqlResult q2 = SelectBuilder.subquery(shared)
    .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref())
    .from(ORDERS)
    .where(eq(ORDERS.STATUS, "PROCESSING"))
    .build();

SqlResult union = UnionBuilder.create(shared)
    .union(q1)
    .union(q2)
    .orderByExpr("o.id", SortDirection.ASC)
    .build();
```

### Correlated Subquery

```java
ParameterBinder shared = new ParameterBinder();

SqlResult sub = SelectBuilder.subquery(shared)
    .select("MAX(o2.amount)")
    .from(ORDERS.as("o2"))
    .where(eqColumn(ORDERS.CUSTOMER_ID, ORDERS.as("o2").CUSTOMER_ID))
    .build();

SelectBuilder.subquery(shared)
    .select(ORDERS.ID.ref())
    .from(ORDERS)
    .where(subquery(ORDERS.AMOUNT, ComparisonOp.EQ, sub))
    .build();
```

### Subquery in FROM

```java
SqlResult sub = SelectBuilder.query()
    .select(ORDERS.CUSTOMER_ID.ref(), ORDERS.AMOUNT.sumAs("total"))
    .from(ORDERS)
    .groupBy(ORDERS.CUSTOMER_ID)
    .build();

SelectBuilder.query()
    .select("s.customer_id", "s.total")
    .fromSubquery(sub, "s")
    .where(raw("s.total > ?", 5000))
    .build();
```

### EXISTS / IN Subquery

```java
ParameterBinder shared = new ParameterBinder();

SqlResult activeCustomers = SelectBuilder.subquery(shared)
    .select(CUSTOMERS.ID.ref())
    .from(CUSTOMERS)
    .where(eq(CUSTOMERS.TIER, "GOLD"))
    .build();

// EXISTS
SelectBuilder.subquery(shared)
    .select(ORDERS.ID.ref())
    .from(ORDERS)
    .where(exists(activeCustomers))
    .build();

// IN subquery
SelectBuilder.subquery(shared)
    .select(ORDERS.ID.ref())
    .from(ORDERS)
    .where(inSubquery(ORDERS.CUSTOMER_ID, activeCustomers))
    .build();
```

### Aggregates

```java
SelectBuilder.query()
    .select(
        ORDERS.STATUS.ref(),
        ORDERS.AMOUNT.sumAs("total_amount"),
        ORDERS.ID.countAs("order_count")
    )
    .from(ORDERS)
    .groupBy(ORDERS.STATUS)
    .having(raw("COUNT(o.id) > ?", 10))
    .orderBy(ORDERS.STATUS, SortDirection.ASC)
    .build();
```
