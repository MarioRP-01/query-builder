# DML Builders Reference

## InsertBuilder

### Type-Safe SET API

```java
SqlResult result = InsertBuilder.insert()
    .into(ORDERS)
    .set(ORDERS.ID, 1001L)
    .set(ORDERS.STATUS, "PENDING")
    .set(ORDERS.AMOUNT, new BigDecimal("250.00"))
    .build();
// INSERT INTO orders (id, status, amount) VALUES (:id_1, :status_2, :amount_3)
```

Type safety enforced: `set(Column<T>, T)` — compiler rejects `set(ORDERS.ID, "not a long")`.

### Columnar API

```java
SqlResult result = InsertBuilder.insert()
    .into(ORDERS)
    .columns(ORDERS.ID, ORDERS.STATUS, ORDERS.AMOUNT)
    .values(1001L, "PENDING", new BigDecimal("250.00"))
    .build();
```

### Multi-Row Insert (Oracle INSERT ALL)

```java
SqlResult result = InsertBuilder.insert()
    .into(ORDERS)
    .columns(ORDERS.ID, ORDERS.STATUS)
    .values(1L, "A")
    .values(2L, "B")
    .values(3L, "C")
    .build();
// INSERT ALL
//   INTO orders (id, status) VALUES (:id_1, :status_2)
//   INTO orders (id, status) VALUES (:id_3, :status_4)
//   INTO orders (id, status) VALUES (:id_5, :status_6)
// SELECT 1 FROM DUAL
```

### INSERT FROM SELECT

```java
SqlResult source = SelectBuilder.query()
    .select(ORDERS.ID.ref(), ORDERS.STATUS.ref())
    .from(ORDERS)
    .where(eq(ORDERS.STATUS, "PENDING"))
    .build();

SqlResult insert = InsertBuilder.insert()
    .into(ARCHIVE)
    .columns(ARCHIVE.ID, ARCHIVE.STATUS)
    .insertFrom(source)
    .build();
// INSERT INTO archive (id, status) SELECT o.id, o.status FROM orders o WHERE ...
```

### NULL Handling

```java
// values() rejects nulls — use valuesOrNull() for explicit NULLs
InsertBuilder.insert()
    .into(ORDERS)
    .columns(ORDERS.ID, ORDERS.REGION)
    .valuesOrNull(1L, null)
    .build();
// INSERT INTO orders (id, region) VALUES (:id_1, NULL)
```

### RETURNING INTO (Oracle)

```java
InsertBuilder.insert()
    .into(ORDERS)
    .set(ORDERS.STATUS, "NEW")
    .returning(ORDERS.ID)
    .build();
// INSERT INTO orders (status) VALUES (:status_1) RETURNING id INTO :id
```

### Template Mode (Spring Batch)

```java
SqlResult template = InsertBuilder.insert()
    .into(ORDERS)
    .set(ORDERS.ID, 0L)          // value ignored in template
    .set(ORDERS.STATUS, "X")     // value ignored in template
    .buildTemplate();
// SQL:    INSERT INTO orders (id, status) VALUES (:id, :status)
// Params: {} (empty — filled per-item by Spring Batch)
```

---

## UpdateBuilder

### Basic Update

```java
SqlResult result = UpdateBuilder.update()
    .table(ORDERS)
    .set(ORDERS.STATUS, "SHIPPED")
    .set(ORDERS.AMOUNT, new BigDecimal("500"))
    .where(eq(ORDERS.ID, 1001L))
    .build();
// UPDATE orders o SET status = :status_1, amount = :amount_2
// WHERE o.id = :id_3
```

### SET Variants

```java
.set(ORDERS.STATUS, "SHIPPED")              // SET status = :status_1
.setNull(ORDERS.REGION)                     // SET region = NULL
.setIfPresent(ORDERS.STATUS, maybeNull)     // skipped if null
.setSubquery(ORDERS.AMOUNT, subResult)      // SET amount = (SELECT ...)
```

### WHERE Safety Guard

`build()` **requires** at least one WHERE condition. This prevents accidental full-table updates:

```java
// THROWS IllegalStateException
UpdateBuilder.update().table(ORDERS).set(ORDERS.STATUS, "X").build();

// Explicit opt-in for full-table update
UpdateBuilder.update().table(ORDERS).set(ORDERS.STATUS, "X").buildUnconditional();
```

### SET vs WHERE Qualification

SET clauses use **unqualified** column names; WHERE uses **qualified** (alias.column):

```sql
UPDATE orders o SET status = :status_1 WHERE o.id = :id_2
--                  ^^^^^^ unqualified        ^^^^ qualified
```

This matches Oracle's expected syntax.

### Template Mode

```java
SqlResult template = UpdateBuilder.update()
    .table(ORDERS)
    .set(ORDERS.STATUS, "X")
    .where(eq(ORDERS.ID, 0L))
    .buildTemplate();
// SQL:    UPDATE orders o SET status = :status WHERE o.id = :id
// Params: {} (filled per-item)
```

---

## DeleteBuilder

### Basic Delete

```java
SqlResult result = DeleteBuilder.delete()
    .from(ORDERS)
    .where(eq(ORDERS.STATUS, "CANCELLED"))
    .build();
// DELETE FROM orders o WHERE o.status = :status_1
```

### WHERE Safety Guard

Same as UpdateBuilder — `build()` requires WHERE. Use `buildUnconditional()` to delete all rows.

```java
// Full-table delete (explicit opt-in)
DeleteBuilder.delete().from(ORDERS).buildUnconditional();
```

### RETURNING INTO (Oracle)

```java
DeleteBuilder.delete()
    .from(ORDERS)
    .where(eq(ORDERS.ID, 1001L))
    .returning(ORDERS.ID, ORDERS.STATUS)
    .build();
// DELETE FROM orders o WHERE o.id = :id_1 RETURNING id, status INTO :id, :status
```

---

## MergeBuilder (Oracle MERGE)

### MERGE with DUAL Source

For upsert by primary key with literal values:

```java
SqlResult result = MergeBuilder.merge()
    .into(ORDERS)
    .usingDual(
        new ColumnValue<>(ORDERS.ID, 1001L),
        new ColumnValue<>(ORDERS.AMOUNT, new BigDecimal("500")))
    .on(ORDERS.ID)
    .whenMatchedUpdate(ORDERS.AMOUNT)
    .whenNotMatchedInsert(ORDERS.ID, ORDERS.AMOUNT)
    .build();
// MERGE INTO orders o
// USING (SELECT :id_1 AS id, :amount_2 AS amount FROM DUAL) src
// ON (o.id = src.id)
// WHEN MATCHED THEN UPDATE SET o.amount = src.amount
// WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (src.id, src.amount)
```

### MERGE with Subquery Source

```java
SqlResult source = SelectBuilder.query()
    .select(STAGING.ID.ref(), STAGING.AMOUNT.ref())
    .from(STAGING)
    .build();

SqlResult merge = MergeBuilder.merge()
    .into(ORDERS)
    .usingSubquery(source, "src")
    .on(ORDERS.ID)
    .whenMatchedUpdate(ORDERS.AMOUNT)
    .whenMatchedDelete()                          // Oracle 10g+ extension
    .whenNotMatchedInsert(ORDERS.ID, ORDERS.AMOUNT)
    .build();
```

### WHEN MATCHED with Literal Values

```java
.whenMatchedSet(ORDERS.STATUS, "PROCESSED")   // SET o.status = :status_N
```

### ColumnValue Type Safety

```java
// ColumnValue<T> preserves type: Column<T> + T
new ColumnValue<>(ORDERS.ID, 1001L)           // OK: Column<Long> + Long
new ColumnValue<>(ORDERS.ID, "not a long")    // compile error
new ColumnValue<>(ORDERS.ID, null)            // NPE at construction
```

---

## Common Patterns

### Batch INSERT → Template

```java
// Provider creates template once; Spring Batch fills values per item
@Bean
public BatchDmlProvider insertProvider() {
    return params -> InsertBuilder.insert()
        .into(ORDERS)
        .set(ORDERS.ID, 0L)
        .set(ORDERS.STATUS, "X")
        .set(ORDERS.AMOUNT, BigDecimal.ZERO)
        .buildTemplate();
}
```

### Conditional UPDATE

```java
UpdateBuilder.update()
    .table(ORDERS)
    .set(ORDERS.STATUS, "SHIPPED")
    .setIfPresent(ORDERS.REGION, newRegion)    // only if non-null
    .where(
        eq(ORDERS.ID, orderId),
        eqIfPresent(ORDERS.STATUS, expectedStatus)
    )
    .build();
```
