# Getting Started

## Requirements

- Java 17+
- Maven (for full build with Spring Batch)
- Oracle database (target dialect)

## Define a Table

Every database table needs a corresponding Java class extending `Table`. Columns are typed and must be assigned in the constructor — this enables alias rebinding via `as()`.

```java
import com.enterprise.batch.sql.core.Table;
import com.enterprise.batch.sql.core.Column;

public final class OrderTable extends Table {
    // Singleton with default alias
    public static final OrderTable ORDERS = new OrderTable("o");

    public final Column<Long>       ID;
    public final Column<BigDecimal> AMOUNT;
    public final Column<String>     STATUS;
    public final Column<Long>       CUSTOMER_ID;
    public final Column<LocalDate>  CREATED_DATE;

    public OrderTable(String alias) {
        super("orders", alias);
        this.ID           = column("id", Long.class);
        this.AMOUNT       = column("amount", BigDecimal.class);
        this.STATUS       = column("status", String.class);
        this.CUSTOMER_ID  = column("customer_id", Long.class);
        this.CREATED_DATE = column("created_date", LocalDate.class);
    }

    @Override
    public OrderTable as(String newAlias) {
        return new OrderTable(newAlias);  // fresh columns bound to new alias
    }
}
```

Rules:
- **Columns in constructor only** — never declare as inline initializers. `as()` won't rebind them otherwise.
- **`as()` must return new instance** — enables self-joins and alias-safe subqueries.
- **Use the singleton** (`ORDERS`) for typical queries. Call `.as("o2")` only for self-joins.

## First SELECT

```java
import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.order.domain.OrderTable.ORDERS;

import com.enterprise.batch.sql.builder.SelectBuilder;
import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.sql.core.SortDirection;

SqlResult result = SelectBuilder.query()
    .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), ORDERS.STATUS.ref())
    .from(ORDERS)
    .where(eq(ORDERS.STATUS, "PENDING"))
    .orderBy(ORDERS.CREATED_DATE, SortDirection.DESC)
    .build();

System.out.println(result.sql());
// SELECT o.id, o.amount, o.status FROM orders o
// WHERE o.status = :status_1
// ORDER BY o.created_date DESC

System.out.println(result.namedParameters());
// {status_1=PENDING}
```

## First INSERT

```java
import com.enterprise.batch.sql.builder.InsertBuilder;

SqlResult insert = InsertBuilder.insert()
    .into(ORDERS)
    .set(ORDERS.ID, 1001L)
    .set(ORDERS.STATUS, "NEW")
    .set(ORDERS.AMOUNT, new BigDecimal("250.00"))
    .build();
// INSERT INTO orders (id, status, amount) VALUES (:id_1, :status_2, :amount_3)
```

## First UPDATE

```java
import com.enterprise.batch.sql.builder.UpdateBuilder;

SqlResult update = UpdateBuilder.update()
    .table(ORDERS)
    .set(ORDERS.STATUS, "SHIPPED")
    .where(eq(ORDERS.ID, 1001L))
    .build();
// UPDATE orders o SET status = :status_1 WHERE o.id = :id_2
```

## First DELETE

```java
import com.enterprise.batch.sql.builder.DeleteBuilder;

SqlResult delete = DeleteBuilder.delete()
    .from(ORDERS)
    .where(eq(ORDERS.STATUS, "CANCELLED"))
    .build();
// DELETE FROM orders o WHERE o.status = :status_1
```

## Using the Result

```java
// Named parameters → Spring NamedParameterJdbcTemplate
String sql = result.sql();
Map<String, Object> params = result.namedParameters();

// Positional parameters → JDBC PreparedStatement
SqlResult.PositionalQuery pq = result.toPositional();
String positionalSql = pq.sql();   // ... WHERE o.status = ?
Object[] values = pq.values();     // ["PENDING"]

// Debug view (values inlined, NOT for execution)
String debug = result.toDebugString();
// ... WHERE o.status = 'PENDING'

// Safety check: all :params in SQL have matching values
result.verify();
```

## Optional Parameters

Use `*IfPresent` methods to build dynamic queries. Null values are silently dropped from WHERE:

```java
String status = params.get("status");       // might be null
BigDecimal minAmount = params.get("min");   // might be null

SqlResult result = SelectBuilder.query()
    .select(ORDERS.ID.ref())
    .from(ORDERS)
    .where(
        eqIfPresent(ORDERS.STATUS, status),        // skipped if null
        gteIfPresent(ORDERS.AMOUNT, minAmount)     // skipped if null
    )
    .build();

// If both null → SELECT o.id FROM orders o (no WHERE)
// If only status set → ... WHERE o.status = :status_1
// If both set → ... WHERE o.status = :status_1 AND o.amount >= :amount_2
```

## JOINs

```java
import static com.enterprise.batch.order.domain.CustomerTable.CUSTOMERS;
import com.enterprise.batch.sql.core.JoinType;

SqlResult result = SelectBuilder.query()
    .select(ORDERS.ID.ref(), CUSTOMERS.NAME.ref())
    .from(ORDERS)
    .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
    .where(eq(CUSTOMERS.TIER, "GOLD"))
    .build();
// SELECT o.id, c.name FROM orders o
// INNER JOIN customers c ON o.customer_id = c.id
// WHERE c.tier = :tier_1
```

## Debugging

```java
import com.enterprise.batch.sql.debug.QueryDebugger;

System.out.println(QueryDebugger.format(result));
// === SQL Query Debug ===
// SQL (named):   SELECT o.id FROM orders o WHERE o.status = :status_1
// SQL (positional): SELECT o.id FROM orders o WHERE o.status = ?
// SQL (values inlined): SELECT o.id FROM orders o WHERE o.status = 'PENDING'
// Parameters (1):
//   status_1 = PENDING (String)
// ======================
```
