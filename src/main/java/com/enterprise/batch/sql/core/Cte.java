package com.enterprise.batch.sql.core;

import com.enterprise.batch.sql.builder.SqlResult;
import com.enterprise.batch.sql.param.ParameterBinder;

/**
 * Abstract base for first-class CTE definitions. Extends {@link Table} so CTE columns,
 * names, and references work with {@code from()}, {@code join()}, and column {@code ref()}
 * the same way regular tables do.
 *
 * <p>Three usage modes:
 * <ul>
 *   <li><b>Fixed</b> — override {@link #buildQuery(ParameterBinder)} with a self-contained query.
 *       Use via {@code .with(cte)}.</li>
 *   <li><b>Parameterized</b> — add a custom {@code buildQuery(binder, ...)} method with extra args.
 *       Use via {@code .with(cte, cte.buildQuery(binder, args))}.</li>
 *   <li><b>Dynamic</b> — define columns only, supply the query externally.
 *       Use via {@code .with(cte, externalQuery)}.</li>
 * </ul>
 *
 * <p>Example:
 * <pre>{@code
 * public final class HighValueCte extends Cte {
 *     public static final HighValueCte HIGH_VALUE = new HighValueCte("hv");
 *     public final Column<Long> CUSTOMER_ID;
 *
 *     public HighValueCte(String alias) {
 *         super("high_value_customers", alias);
 *         this.CUSTOMER_ID = column("customer_id", Long.class);
 *     }
 *
 *     @Override
 *     public SqlResult buildQuery(ParameterBinder binder) {
 *         return SelectBuilder.subquery(binder)
 *             .select(ORDERS.CUSTOMER_ID.ref())
 *             .from(ORDERS)
 *             .groupBy(ORDERS.CUSTOMER_ID)
 *             .build();
 *     }
 *
 *     @Override
 *     public HighValueCte as(String newAlias) { return new HighValueCte(newAlias); }
 * }
 * }</pre>
 */
public abstract class Cte extends Table {

    protected Cte(String cteName, String alias) {
        super(cteName, alias);
    }

    /** Readability alias for {@link #tableName()}. */
    public String cteName() {
        return tableName();
    }

    /**
     * Builds the CTE query using the shared binder. Override for fixed CTEs.
     * Default throws — dynamic/parameterized CTEs must supply the query externally.
     */
    public SqlResult buildQuery(ParameterBinder binder) {
        throw new UnsupportedOperationException(
                "CTE '" + cteName() + "' requires parameters or an external query");
    }
}
