package com.enterprise.batch.sql.validation;

import com.enterprise.batch.sql.core.Column;
import com.enterprise.batch.sql.core.Table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

/**
 * Validates Table definitions against actual database metadata.
 * Requires a live DataSource connection.
 */
public class SchemaValidator {

    private final DataSource dataSource;

    public SchemaValidator(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<String> validate(Table... tables) {
        List<String> errors = new ArrayList<>();
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            for (Table table : tables) {
                validateTable(meta, table, errors);
            }
        } catch (Exception e) {
            errors.add("Failed to connect: " + e.getMessage());
        }
        return errors;
    }

    private void validateTable(DatabaseMetaData meta, Table table, List<String> errors) {
        try (ResultSet rs = meta.getColumns(null, null, table.tableName(), null)) {
            List<String> dbColumns = new ArrayList<>();
            while (rs.next()) {
                dbColumns.add(rs.getString("COLUMN_NAME").toLowerCase());
            }
            if (dbColumns.isEmpty()) {
                errors.add("Table '" + table.tableName() + "' not found in database");
                return;
            }
            for (Column<?> col : table.allColumns()) {
                if (!dbColumns.contains(col.name().toLowerCase())) {
                    errors.add("Column '" + col.name() + "' not found in table '"
                            + table.tableName() + "'");
                }
            }
        } catch (Exception e) {
            errors.add("Error validating table '" + table.tableName() + "': " + e.getMessage());
        }
    }
}
