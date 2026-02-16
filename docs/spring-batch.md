# Spring Batch Integration

## Overview

The `com.enterprise.batch.shared.querybridge` package bridges the SQL DSL with Spring Batch's `ItemReader` / `ItemWriter` infrastructure. It uses a ports & adapters layout: `querybridge.port` holds pure Java contracts (`BatchQueryProvider`, `BatchDmlProvider`) with zero Spring imports, while `querybridge.adapter` holds the Spring Batch bridge (factories, registries, `@Configuration`). The key principle: **providers build fresh queries on every call**, ensuring thread safety and parameter isolation.

```
Provider (stateless)  →  Factory (creates reader/writer)  →  Spring Batch step
```

## Read Side

### BatchQueryProvider

```java
@FunctionalInterface
public interface BatchQueryProvider {
    SqlResult buildQuery(Map<String, Object> jobParams);
}
```

Contract:
- Create a **fresh** `SelectBuilder` on every call
- Never reuse or cache builder instances
- Job parameters are passed in for dynamic query construction

```java
@Bean("pendingOrders")
public BatchQueryProvider pendingOrdersProvider() {
    return params -> SelectBuilder.query()
        .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), CUSTOMERS.NAME.ref())
        .from(ORDERS)
        .innerJoin(CUSTOMERS, ORDERS.CUSTOMER_ID, CUSTOMERS.ID)
        .where(
            eq(ORDERS.STATUS, "PENDING"),
            eqIfPresent(ORDERS.REGION, (String) params.get("region"))
        )
        .orderBy(ORDERS.ID, SortDirection.ASC)
        .build();
}
```

### BatchReaderFactory

Creates `JdbcCursorItemReader<T>` from a provider:

```java
@Bean
@StepScope
public JdbcCursorItemReader<OrderDto> orderReader(
        BatchReaderFactory readerFactory,
        @Value("#{jobParameters['region']}") String region) {

    return readerFactory.cursorReader(
        "orderReader",                    // reader name
        pendingOrdersProvider(),          // query provider
        (rs, i) -> new OrderDto(          // RowMapper
            rs.getLong("id"),
            rs.getBigDecimal("amount"),
            rs.getString("name")
        ),
        Map.of("region", region)          // job params
    );
}
```

Internal steps:
1. Calls `provider.buildQuery(params)` → `SqlResult`
2. Runs `result.verify()` — ensures all `:params` are bound
3. Converts to positional via `result.toPositional()` — `:status_1` → `?`
4. Configures `JdbcCursorItemReader` with positional SQL + `ArgumentPreparedStatementSetter`

### QueryProviderRegistry

Named lookup when a job has multiple readers:

```java
@Bean
public QueryProviderRegistry queryRegistry() {
    QueryProviderRegistry registry = new QueryProviderRegistry();
    registry.register("pendingOrders", pendingOrdersProvider());
    registry.register("overduePayments", overduePaymentsProvider());
    registry.register("activeCustomers", activeCustomersProvider());
    return registry;
}

// Usage
BatchQueryProvider provider = registry.get("pendingOrders");
```

---

## Write Side

### BatchDmlProvider

```java
@FunctionalInterface
public interface BatchDmlProvider {
    SqlResult buildDml(Map<String, Object> jobParams);
}
```

Same contract as `BatchQueryProvider` — fresh builder per call. Uses `buildTemplate()` instead of `build()`:

```java
@Bean("updateOrderStatus")
public BatchDmlProvider updateOrderProvider() {
    return params -> UpdateBuilder.update()
        .table(ORDERS)
        .set(ORDERS.STATUS, "PROCESSED")
        .set(ORDERS.AMOUNT, BigDecimal.ZERO)         // placeholder value
        .where(eq(ORDERS.ID, 0L))                    // placeholder value
        .buildTemplate();
    // SQL:    UPDATE orders o SET status = :status, amount = :amount WHERE o.id = :id
    // Params: {} (empty — filled per-item by ItemSqlParameterSourceProvider)
}
```

### buildTemplate() vs build()

| Method | Parameter names | Parameter values | Use case |
|--------|----------------|------------------|----------|
| `build()` | `:col_N` (with counter) | Bound in map | One-shot execution |
| `buildTemplate()` | `:col` (no counter) | Empty map | Spring Batch per-item filling |

`buildTemplate()` produces `:column_name` placeholders that match bean property names, so `BeanPropertyItemSqlParameterSourceProvider` can fill them automatically.

### BatchWriterFactory

Creates `JdbcBatchItemWriter<T>` from a provider:

```java
@Bean
@StepScope
public JdbcBatchItemWriter<OrderDto> orderWriter(
        BatchWriterFactory writerFactory) {

    return writerFactory.batchWriter(
        "orderWriter",                                      // writer name
        updateOrderProvider(),                               // DML provider
        new BeanPropertyItemSqlParameterSourceProvider<>(),  // param source
        Map.of()                                             // job params
    );
}
```

The `ItemSqlParameterSourceProvider` maps each item's properties to the `:column_name` placeholders. For example, if the item has `getId()` and `getStatus()`, they fill `:id` and `:status`.

### DmlProviderRegistry

Same pattern as `QueryProviderRegistry`:

```java
@Bean
public DmlProviderRegistry dmlRegistry() {
    DmlProviderRegistry registry = new DmlProviderRegistry();
    registry.register("updateOrder", updateOrderProvider());
    registry.register("insertArchive", insertArchiveProvider());
    return registry;
}
```

---

## Auto-Configuration

`SpringBatchQueryConfig` is a `@Configuration` that wires all four beans:

```java
@Configuration
public class SpringBatchQueryConfig {
    @Bean public BatchReaderFactory batchReaderFactory(DataSource ds) { ... }
    @Bean public QueryProviderRegistry queryProviderRegistry() { ... }
    @Bean public BatchWriterFactory batchWriterFactory(DataSource ds) { ... }
    @Bean public DmlProviderRegistry dmlProviderRegistry() { ... }
}
```

Import it:

```java
@SpringBootApplication
@Import(SpringBatchQueryConfig.class)
public class MyBatchApplication { }
```

---

## Complete Job Example

A typical read-process-write batch job:

```java
@Configuration
@EnableBatchProcessing
public class OrderProcessingJobConfig {

    // 1. Query provider (read)
    @Bean
    public BatchQueryProvider pendingOrdersProvider() {
        return params -> SelectBuilder.query()
            .select(ORDERS.ID.ref(), ORDERS.AMOUNT.ref(), ORDERS.STATUS.ref())
            .from(ORDERS)
            .where(eq(ORDERS.STATUS, "PENDING"))
            .orderBy(ORDERS.ID, SortDirection.ASC)
            .build();
    }

    // 2. DML provider (write)
    @Bean
    public BatchDmlProvider updateOrderProvider() {
        return params -> UpdateBuilder.update()
            .table(ORDERS)
            .set(ORDERS.STATUS, "PROCESSED")
            .set(ORDERS.AMOUNT, BigDecimal.ZERO)
            .where(eq(ORDERS.ID, 0L))
            .buildTemplate();
    }

    // 3. Reader
    @Bean
    @StepScope
    public JdbcCursorItemReader<Order> reader(BatchReaderFactory factory) {
        return factory.cursorReader("reader", pendingOrdersProvider(),
            (rs, i) -> new Order(rs.getLong("id"), rs.getBigDecimal("amount")),
            Map.of());
    }

    // 4. Processor
    @Bean
    public ItemProcessor<Order, Order> processor() {
        return order -> {
            order.setStatus("PROCESSED");
            return order;
        };
    }

    // 5. Writer
    @Bean
    @StepScope
    public JdbcBatchItemWriter<Order> writer(BatchWriterFactory factory) {
        return factory.batchWriter("writer", updateOrderProvider(),
            new BeanPropertyItemSqlParameterSourceProvider<>(), Map.of());
    }

    // 6. Step + Job
    @Bean
    public Step processOrdersStep(StepBuilderFactory sbf,
            JdbcCursorItemReader<Order> reader,
            ItemProcessor<Order, Order> processor,
            JdbcBatchItemWriter<Order> writer) {
        return sbf.get("processOrders")
            .<Order, Order>chunk(100)
            .reader(reader)
            .processor(processor)
            .writer(writer)
            .build();
    }

    @Bean
    public Job processOrdersJob(JobBuilderFactory jbf, Step step) {
        return jbf.get("processOrdersJob").start(step).build();
    }
}
```

---

## Thread Safety

Providers **must** create fresh builders on every call. Builders are mutable (they accumulate clauses and bind parameters), so sharing them across threads causes corruption.

```java
// CORRECT — fresh builder per call
return params -> SelectBuilder.query().select(...).from(...).build();

// WRONG — shared builder state
SelectBuilder shared = SelectBuilder.query();  // created once
return params -> shared.select(...).build();   // reused — unsafe!
```

The `ParameterBinder` inside each builder uses `AtomicInteger` for its counter, but the builder itself (clause lists, state) is not synchronized. Fresh-per-call is the intended usage pattern.
