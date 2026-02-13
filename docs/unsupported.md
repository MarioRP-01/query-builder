# Unsupported Use Cases

Consolidated list of SQL features not yet covered by the DSL, grouped by component. Workarounds noted where applicable.

## SelectBuilder

| Feature | Workaround |
|---------|-----------|
| `FOR UPDATE` / `FOR UPDATE SKIP LOCKED` | Use `selectRaw()` with raw SQL suffix, or native query |
| `NULLS FIRST` / `NULLS LAST` in ORDER BY | Use `orderByExpr("col ASC NULLS FIRST", ASC)` (validate manually) |
| Window functions (`ROW_NUMBER`, `RANK`, `LAG`, `LEAD`) | Use `selectRaw()` for the window expression |
| `CASE WHEN ... THEN ... ELSE ... END` in SELECT | Use `selectRaw()` for the CASE expression |
| `RECURSIVE` CTEs | Use `with()` + raw recursive SQL via subquery builder |
| Typed HAVING on aggregates | Use `havingRaw("SUM(o.amount) >= ?", 1000)` |

## UnionBuilder

| Feature | Workaround |
|---------|-----------|
| `EXCEPT` / `MINUS` | Use `NOT EXISTS` subquery condition |
| `INTERSECT` | Use `EXISTS` subquery condition |

## UpdateBuilder

| Feature | Workaround |
|---------|-----------|
| UPDATE with JOIN (multi-table) | Use correlated subquery in `setSubquery()` |
| `SET col = col + expr` (arithmetic) | Not supported; use native SQL |
| `SET col = CASE WHEN ...` (conditional) | Not supported; use native SQL |

## MergeBuilder

| Feature | Workaround |
|---------|-----------|
| `WHEN MATCHED AND condition THEN` (conditional match) | Not supported; filter in source subquery |
| `WHEN NOT MATCHED AND condition THEN INSERT` | Not supported; filter in source subquery |

## Conditions

| Feature | Workaround |
|---------|-----------|
| `endsWith(col, suffix)` | `like(col, "%" + suffix)` |
| `NOT BETWEEN` | `or(lt(col, low), gt(col, high))` |
| `LIKE ESCAPE` clause | `raw("col LIKE ? ESCAPE '\\'", pattern)` |
