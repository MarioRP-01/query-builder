# QueryBuilder Documentation

Type-safe SQL DSL for Oracle + Spring Batch. Generates JDBC-ready SQL with named parameters, composable conditions, and compile-time safety.

## Contents

| Document | Description |
|----------|-------------|
| [Architecture](architecture.md) | Component overview, package structure, data flow |
| [Getting Started](getting-started.md) | Setup, table definitions, first query |
| [SELECT Builder](select-builder.md) | SELECT, JOIN, CTE, UNION, subqueries, pagination |
| [DML Builders](dml-builders.md) | INSERT, UPDATE, DELETE, MERGE reference |
| [Conditions](conditions.md) | Predicate DSL: comparisons, composition, nulls, subqueries |
| [Spring Batch Integration](spring-batch.md) | Readers, writers, providers, registries |
| [Design Decisions](design-decisions.md) | Patterns, trade-offs, rationale |
| [Security](security.md) | SQL injection protection, expression validation |
| [Unsupported](unsupported.md) | Features not yet covered, with workarounds |

## Quick Example

```java
import static com.enterprise.batch.sql.condition.Conditions.*;
import static com.enterprise.batch.example.tables.OrderTable.ORDERS;
import static com.enterprise.batch.example.tables.CustomerTable.CUSTOMERS;

SqlResult result = SelectBuilder.query()
    .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), CUSTOMERS.NAME.ref())
    .from(ORDERS)
    .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
    .where(
        eq(ORDERS.STATUS, "PENDING"),
        gteIfPresent(ORDERS.AMOUNT, minAmount)  // skipped if null
    )
    .orderBy(ORDERS.CREATED_DATE, SortDirection.DESC)
    .build();

// Named:  SELECT o.id, o.amount, c.name FROM orders o
//         INNER JOIN customers c ON o.customer_id = c.id
//         WHERE o.status = :status_1 AND o.amount >= :amount_2
//         ORDER BY o.created_date DESC

// Parameters: {status_1="PENDING", amount_2=100}
```

## Build & Test

```bash
mvn compile                    # compile all
mvn compile exec:java -Dexec.mainClass="com.enterprise.batch.sql.AllGapsTest"       # 25 tests
mvn compile exec:java -Dexec.mainClass="com.enterprise.batch.sql.EdgeCaseTests"     # 66 tests
mvn compile exec:java -Dexec.mainClass="com.enterprise.batch.sql.SpringBatchTests"  # 8 tests
mvn test-compile exec:java -Dexec.mainClass="com.enterprise.batch.sql.DmlBuilderTests"      # 33 tests
mvn test-compile exec:java -Dexec.mainClass="com.enterprise.batch.sql.OrderEnrichmentTest"  # 10 tests
```

Java 17+. Oracle target DB. Tests are standalone (no JUnit) — each `main()` exits with code 1 on failure.
