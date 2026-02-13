# Conditions Reference

All conditions implement `Condition` — a functional interface with one method:

```java
@FunctionalInterface
public interface Condition {
    String toSql(ParameterBinder binder);
}
```

All factory methods live in the `Conditions` class. Import statically for clean DSL:

```java
import static com.enterprise.batch.sql.condition.Conditions.*;
```

## Strict vs Optional

The condition API is split into two categories:

| Category | Null behavior | Use case |
|----------|---------------|----------|
| **Strict** (`eq`, `gt`, ...) | Throws NPE | Required filters — null is a bug |
| **Optional** (`eqIfPresent`, ...) | Returns `null` | Dynamic filters — null means "don't filter" |

`.where()` silently filters out `null` conditions, so optional methods integrate cleanly:

```java
.where(
    eq(ORDERS.STATUS, "ACTIVE"),                // required — NPE if null
    gteIfPresent(ORDERS.AMOUNT, userMinAmount)  // optional — skipped if null
)
```

## Comparison Conditions

### Strict (NPE on null)

| Method | SQL | Example |
|--------|-----|---------|
| `eq(col, value)` | `col = :v` | `eq(ORDERS.STATUS, "A")` |
| `neq(col, value)` | `col <> :v` | `neq(ORDERS.STATUS, "CANCELLED")` |
| `gt(col, value)` | `col > :v` | `gt(ORDERS.AMOUNT, 100)` |
| `gte(col, value)` | `col >= :v` | `gte(ORDERS.AMOUNT, 100)` |
| `lt(col, value)` | `col < :v` | `lt(ORDERS.AMOUNT, 1000)` |
| `lte(col, value)` | `col <= :v` | `lte(ORDERS.AMOUNT, 1000)` |
| `between(col, lo, hi)` | `col BETWEEN :lo AND :hi` | `between(ORDERS.AMOUNT, 100, 1000)` |
| `like(col, pattern)` | `col LIKE :v` | `like(ORDERS.STATUS, "PEND%")` |
| `notLike(col, pattern)` | `col NOT LIKE :v` | `notLike(ORDERS.STATUS, "%TEST%")` |
| `contains(col, text)` | `col LIKE '%text%'` | `contains(ORDERS.STATUS, "END")` |
| `startsWith(col, text)` | `col LIKE 'text%'` | `startsWith(ORDERS.STATUS, "P")` |
| `in(col, list)` | `col IN (:v1, :v2, ...)` | `in(ORDERS.STATUS, List.of("A", "B"))` |
| `notIn(col, list)` | `col NOT IN (:v1, :v2, ...)` | `notIn(ORDERS.STATUS, List.of("X"))` |

### Optional (null → skipped)

| Method | Behavior when null |
|--------|-------------------|
| `eqIfPresent(col, value)` | Returns `null` → skipped by `.where()` |
| `gteIfPresent(col, value)` | Same |
| `lteIfPresent(col, value)` | Same |
| `betweenIfPresent(col, lo, hi)` | Skipped if either bound is null |
| `containsIfPresent(col, text)` | Same |
| `inIfPresent(col, list)` | Skipped if list is null or empty |

## NULL Conditions

```java
isNull(ORDERS.DELETED_AT)       // o.deleted_at IS NULL
isNotNull(ORDERS.DELETED_AT)    // o.deleted_at IS NOT NULL
```

No parameters bound — pure SQL predicate.

## Column-to-Column Comparison

```java
eqColumn(ORDERS.CUSTOMER_ID, CUSTOMERS.ID)         // o.customer_id = c.id
columnOp(ORDERS.AMOUNT, ComparisonOp.GT, PAYMENTS.AMOUNT)  // o.amount > p.amount
```

Used in ON clauses and correlated conditions. No parameters — both sides are column refs.

## Composite Conditions (AND / OR)

### Strict

```java
and(cond1, cond2, cond3)    // (cond1 AND cond2 AND cond3)
or(cond1, cond2)             // (cond1 OR cond2)
```

All children must be non-null; throws if any is null.

### Optional

```java
andIfAny(cond1, null, cond3)  // (cond1 AND cond3) — nulls filtered
orIfAny(null, null, null)     // returns null — all children were null
```

Filters null children first. Returns `null` if no children remain (which `.where()` then skips).

### Nesting

Conditions nest arbitrarily:

```java
.where(
    or(
        and(
            eq(ORDERS.STATUS, "PENDING"),
            gte(ORDERS.AMOUNT, 1000)
        ),
        and(
            eq(ORDERS.STATUS, "PROCESSING"),
            eq(ORDERS.REGION, "EU")
        )
    )
)
// WHERE ((o.status = :status_1 AND o.amount >= :amount_2)
//   OR (o.status = :status_3 AND o.region = :region_4))
```

### Dynamic Composite Filters

Build composite conditions from user input:

```java
Condition statusFilter = orIfAny(
    eqIfPresent(ORDERS.STATUS, params.get("status1")),
    eqIfPresent(ORDERS.STATUS, params.get("status2")),
    eqIfPresent(ORDERS.STATUS, params.get("status3"))
);
// Returns OR of whichever statuses are non-null, or null if all are null

.where(
    statusFilter,    // null → skipped
    gteIfPresent(ORDERS.AMOUNT, params.get("minAmount"))
)
```

## Subquery Conditions

```java
ParameterBinder shared = new ParameterBinder();

SqlResult sub = SelectBuilder.subquery(shared)
    .select(CUSTOMERS.ID.ref())
    .from(CUSTOMERS)
    .where(eq(CUSTOMERS.TIER, "GOLD"))
    .build();

// IN subquery
inSubquery(ORDERS.CUSTOMER_ID, sub)
// o.customer_id IN (SELECT c.id FROM customers c WHERE c.tier = :tier_1)

// NOT IN subquery
notInSubquery(ORDERS.CUSTOMER_ID, sub)

// Comparison subquery
subquery(ORDERS.AMOUNT, ComparisonOp.GT, sub)
// o.amount > (SELECT ...)

// EXISTS
exists(sub)            // EXISTS (SELECT c.id FROM customers c ...)
notExists(sub)         // NOT EXISTS (SELECT ...)
```

**Important**: use `SelectBuilder.subquery(shared)` to share the parent's `ParameterBinder`. This ensures parameter names are globally unique.

## Raw Conditions

For expressions not covered by the typed API:

```java
raw("SUBSTR(o.name, 1, 1) = ?", "J")
// SUBSTR(o.name, 1, 1) = :raw_1

raw("o.amount BETWEEN ? AND ? + 100", 500)
// (uses positional ? placeholders, converted to named)
```

Raw SQL is validated by `ExpressionValidator` — DML keywords, comments, and semicolons are blocked. Values are always parameterized.

## Generic Bounds

Comparison methods use `Comparable<? super V>` to support types like `LocalDate` (which implements `Comparable<ChronoLocalDate>`, not `Comparable<LocalDate>`):

```java
// Works correctly because of <? super V> bound:
gte(ORDERS.CREATED_DATE, LocalDate.now())
between(ORDERS.CREATED_DATE, startDate, endDate)
```

## Implementation Classes

| Condition Type | Class | Key Fields |
|---------------|-------|------------|
| Simple comparison | `SimpleCondition` | `Column`, `ComparisonOp`, `value` |
| Composite AND/OR | `CompositeCondition` | `List<Condition>`, `operator` |
| BETWEEN | `BetweenCondition` | `Column`, `low`, `high` |
| LIKE / NOT LIKE | `LikeCondition` | `Column`, `pattern`, `negated` |
| IN / NOT IN | `InListCondition` | `Column`, `List<values>`, `negated` |
| IS NULL / NOT NULL | `NullCondition` | `Column`, `negated` |
| Column comparison | `ColumnCondition` | `leftCol`, `op`, `rightCol` |
| Subquery | `SubqueryCondition` | `Column`, `op`, `SqlResult` |
| EXISTS | `ExistsCondition` | `SqlResult`, `negated` |
| Raw SQL | `RawCondition` | `sql`, `values[]` |
