# Unsupported Use Cases

Consolidated list of SQL features not yet covered by the DSL, grouped by component. Workarounds noted where applicable.

## SelectBuilder

| Feature | Workaround |
|---------|-----------|
| ~~`FOR UPDATE` / `FOR UPDATE SKIP LOCKED`~~ | **Now supported** via `forUpdate()`, `forUpdateNoWait()`, `forUpdateSkipLocked()` |
| ~~`NULLS FIRST` / `NULLS LAST` in ORDER BY~~ | **Now supported** via `orderBy(col, dir, NullsOrder.NULLS_FIRST)` |
| ~~Window functions (`ROW_NUMBER`, `RANK`, `LAG`, `LEAD`)~~ | **Now supported** via `selectExpr()` + `Over.rowNumber()` / `Over.sum()` / etc. |
| ~~`CASE WHEN ... THEN ... ELSE ... END`~~ | **Now supported** via `selectExpr()` + `Cases.when()` / `Cases.of()` |
| `RECURSIVE` CTEs | Use `with()` + raw recursive SQL via subquery builder |
| Typed HAVING on aggregates | Use `havingRaw("SUM(o.amount) >= ?", 1000)` |

## UnionBuilder

| Feature | Workaround |
|---------|-----------|
| ~~`EXCEPT` / `MINUS`~~ | **Now supported** via `UnionBuilder.except()` |
| ~~`INTERSECT`~~ | **Now supported** via `UnionBuilder.intersect()` |

## UpdateBuilder

| Feature | Workaround |
|---------|-----------|
| UPDATE with JOIN (multi-table) | Use correlated subquery in `setSubquery()` |

## MergeBuilder

| Feature | Workaround |
|---------|-----------|
| `WHEN MATCHED AND condition THEN` (conditional match) | Not supported; filter in source subquery |
| `WHEN NOT MATCHED AND condition THEN INSERT` | Not supported; filter in source subquery |

## Conditions

| Feature | Workaround |
|---------|-----------|
| ~~`endsWith(col, suffix)`~~ | **Now supported** via `endsWith()` / `endsWithIfPresent()` |
| ~~`NOT BETWEEN`~~ | **Now supported** via `notBetween()` / `notBetweenIfPresent()` |
| `LIKE ESCAPE` clause | `raw("col LIKE ? ESCAPE '\\'", pattern)` |
