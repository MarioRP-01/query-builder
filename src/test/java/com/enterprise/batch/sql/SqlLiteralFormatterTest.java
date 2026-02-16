package com.enterprise.batch.sql;

import com.enterprise.batch.sql.param.SqlLiteralFormatter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.*;

class SqlLiteralFormatterTest {

    @Test
    void string() {
        assertThat(SqlLiteralFormatter.format("hello")).isEqualTo("'hello'");
    }

    @Test
    void stringWithQuotes() {
        assertThat(SqlLiteralFormatter.format("it's")).isEqualTo("'it''s'");
    }

    @Test
    void longValue() {
        assertThat(SqlLiteralFormatter.format(42L)).isEqualTo("42");
    }

    @Test
    void integerValue() {
        assertThat(SqlLiteralFormatter.format(7)).isEqualTo("7");
    }

    @Test
    void bigDecimal() {
        assertThat(SqlLiteralFormatter.format(new BigDecimal("123.45"))).isEqualTo("123.45");
    }

    @Test
    void bigDecimalScientific() {
        // 1E+3 should render as 1000, not scientific notation
        assertThat(SqlLiteralFormatter.format(new BigDecimal("1E+3"))).isEqualTo("1000");
    }

    @Test
    void booleanTrue() {
        assertThat(SqlLiteralFormatter.format(true)).isEqualTo("1");
    }

    @Test
    void booleanFalse() {
        assertThat(SqlLiteralFormatter.format(false)).isEqualTo("0");
    }

    @Test
    void localDate() {
        assertThat(SqlLiteralFormatter.format(LocalDate.of(2024, 3, 15)))
                .isEqualTo("DATE '2024-03-15'");
    }

    @Test
    void nullThrows() {
        assertThatThrownBy(() -> SqlLiteralFormatter.format(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void unsupportedTypeThrows() {
        assertThatThrownBy(() -> SqlLiteralFormatter.format(new Object()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported literal type");
    }
}
