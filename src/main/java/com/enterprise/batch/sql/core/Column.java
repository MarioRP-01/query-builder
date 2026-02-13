package com.enterprise.batch.sql.core;

public class Column<T> {

    private final Table table;
    private final String name;
    private final Class<T> type;

    public Column(Table table, String name, Class<T> type) {
        this.table = table;
        this.name = name;
        this.type = type;
    }

    /** Qualified reference: alias.column_name */
    public String ref() {
        return table.alias() + "." + name;
    }

    /** Qualified reference with an AS alias */
    public String refAs(String alias) {
        return ref() + " AS " + alias;
    }

    public String name() { return name; }
    public Class<T> type() { return type; }
    public Table table() { return table; }

    /** Column = Column expression for ON clauses */
    public String eqColumn(Column<?> other) {
        return ref() + " = " + other.ref();
    }

    // Aggregate helpers
    public String countAs(String alias) { return "COUNT(" + ref() + ") AS " + alias; }
    public String sumAs(String alias)   { return "SUM(" + ref() + ") AS " + alias; }
    public String avgAs(String alias)   { return "AVG(" + ref() + ") AS " + alias; }
    public String minAs(String alias)   { return "MIN(" + ref() + ") AS " + alias; }
    public String maxAs(String alias)   { return "MAX(" + ref() + ") AS " + alias; }

    @Override
    public String toString() {
        return ref();
    }
}
