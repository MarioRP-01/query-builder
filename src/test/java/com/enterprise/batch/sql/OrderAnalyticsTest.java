package com.enterprise.batch.sql;

import com.enterprise.batch.BatchApplication;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end test for the order analytics batch job.
 *
 * <p>7 orders (non-CANCELLED) → window analytics → processor derives trend/velocity
 * → writes to order_analytics table + CSV.
 *
 * <p>Data layout (by customer, chronological):
 * <pre>
 * Customer 1 (Acme, US):       O1($250) → O6($800) → O4($3200)
 * Customer 2 (GlobalTech, EU): O2($1500) → O5($499.99) → O7($2100)
 * Customer 3 (MegaStore, APAC): O3($75.50)
 * </pre>
 */
@SpringBootTest(classes = BatchApplication.class)
@TestPropertySource(properties = "spring.batch.job.enabled=false")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OrderAnalyticsTest {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("orderAnalyticsJob")
    private Job orderAnalyticsJob;

    @Autowired
    private DataSource dataSource;

    private JobExecution jobExecution;
    private StepExecution stepExecution;

    @BeforeAll
    void runJob() throws Exception {
        jobExecution = jobLauncher.run(orderAnalyticsJob,
            new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters());
        stepExecution = jobExecution.getStepExecutions().iterator().next();
    }

    @AfterAll
    void cleanup() {
        try { Files.deleteIfExists(Path.of("output/order_analytics.csv")); } catch (Exception ignored) {}
    }

    @Test
    void jobCompletes() {
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
    }

    @Test
    void reads7Orders() {
        assertThat(stepExecution.getReadCount()).isEqualTo(7L);
    }

    @Test
    void writes7AnalyzedRecords() {
        assertThat(stepExecution.getWriteCount()).isEqualTo(7L);
    }

    @Test
    void analyticsTableHas7Rows() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM order_analytics")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(7L);
        }
    }

    // --- Customer 1 (Acme, US): O1($250,01-15) → O6($800,01-20) → O4($3200,02-15) ---

    // O1: seq=1, running=250, prev=0, trend=FIRST
    @Test
    void order1_firstInSequence() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM order_analytics WHERE order_id = 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("customer_order_seq")).isEqualTo(1L);
            assertThat(rs.getBigDecimal("customer_running_total")).isEqualByComparingTo("250.00");
            assertThat(rs.getBigDecimal("prev_amount")).isEqualByComparingTo("0");
            assertThat(rs.getString("trend")).isEqualTo("FIRST");
        }
    }

    // O6: seq=2, running=1050, prev=250, 800>250 → trend=UP
    @Test
    void order6_upTrend() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM order_analytics WHERE order_id = 6")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("customer_order_seq")).isEqualTo(2L);
            assertThat(rs.getBigDecimal("customer_running_total")).isEqualByComparingTo("1050.00");
            assertThat(rs.getBigDecimal("prev_amount")).isEqualByComparingTo("250.00");
            assertThat(rs.getString("trend")).isEqualTo("UP");
        }
    }

    // O4: seq=3, running=4250, prev=800, 3200>800 → trend=UP
    @Test
    void order4_runningTotalAndTrend() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM order_analytics WHERE order_id = 4")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("customer_order_seq")).isEqualTo(3L);
            assertThat(rs.getBigDecimal("customer_running_total")).isEqualByComparingTo("4250.00");
            assertThat(rs.getBigDecimal("prev_amount")).isEqualByComparingTo("800.00");
            assertThat(rs.getString("trend")).isEqualTo("UP");
        }
    }

    // --- Customer 2 (GlobalTech, EU): O2($1500) → O5($499.99) → O7($2100) ---

    // O5: seq=2, prev=1500, 499.99<1500 → trend=DOWN
    @Test
    void order5_downTrend() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM order_analytics WHERE order_id = 5")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("customer_order_seq")).isEqualTo(2L);
            assertThat(rs.getBigDecimal("prev_amount")).isEqualByComparingTo("1500.00");
            assertThat(rs.getString("trend")).isEqualTo("DOWN");
        }
    }

    // O7: seq=3, running=4099.99, prev=499.99, 2100>499.99 → trend=UP
    @Test
    void order7_recoveryTrend() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM order_analytics WHERE order_id = 7")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("customer_order_seq")).isEqualTo(3L);
            assertThat(rs.getBigDecimal("customer_running_total")).isEqualByComparingTo("4099.99");
            assertThat(rs.getString("trend")).isEqualTo("UP");
        }
    }

    // --- Region rankings (DENSE_RANK by amount DESC within region) ---

    // US: O4($3200)→1, O6($800)→2, O1($250)→3
    @Test
    void regionRank_usOrders() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT order_id, region_amount_rank FROM order_analytics " +
                 "WHERE region = 'US' ORDER BY region_amount_rank")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("order_id")).isEqualTo(4L);
            assertThat(rs.getLong("region_amount_rank")).isEqualTo(1L);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("order_id")).isEqualTo(6L);
            assertThat(rs.getLong("region_amount_rank")).isEqualTo(2L);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("order_id")).isEqualTo(1L);
            assertThat(rs.getLong("region_amount_rank")).isEqualTo(3L);
        }
    }

    // --- Velocity flag (RATIO_TO_REPORT → DOMINANT/SIGNIFICANT/MINOR) ---

    // O4: 3200/4250 ≈ 0.753 → DOMINANT
    @Test
    void order4_dominantVelocity() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT velocity_flag FROM order_analytics WHERE order_id = 4")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("velocity_flag")).isEqualTo("DOMINANT");
        }
    }

    // O1: 250/4250 ≈ 0.059 → MINOR
    @Test
    void order1_minorVelocity() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT velocity_flag FROM order_analytics WHERE order_id = 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("velocity_flag")).isEqualTo("MINOR");
        }
    }

    // O3: sole APAC order → ratio=1.0 → DOMINANT
    @Test
    void order3_soleInRegionIsDominant() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT velocity_flag, region_spend_pct FROM order_analytics WHERE order_id = 3")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal("region_spend_pct")).isEqualByComparingTo("1.0");
            assertThat(rs.getString("velocity_flag")).isEqualTo("DOMINANT");
        }
    }

    // --- Spend quartile (NTILE(4) over all 7 orders by amount DESC) ---
    // $3200→Q1, $2100→Q1, $1500→Q2, $800→Q2, $499.99→Q3, $250→Q3, $75.50→Q4
    @Test
    void spendQuartiles() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT order_id, spend_quartile FROM order_analytics ORDER BY spend_quartile, order_id")) {
            // Q1: orders 4 and 7
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("order_id")).isEqualTo(4L);
            assertThat(rs.getLong("spend_quartile")).isEqualTo(1L);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("order_id")).isEqualTo(7L);
            assertThat(rs.getLong("spend_quartile")).isEqualTo(1L);
            // Q4: order 3
            while (rs.next()) {
                if (rs.getLong("order_id") == 3L) {
                    assertThat(rs.getLong("spend_quartile")).isEqualTo(4L);
                }
            }
        }
    }

    // --- CSV output ---

    @Test
    void csvFileExists() {
        assertThat(Files.exists(Path.of("output/order_analytics.csv"))).isTrue();
    }

    @Test
    void csvHasHeaderAnd7DataRows() throws Exception {
        List<String> lines = Files.readAllLines(Path.of("output/order_analytics.csv"));
        assertThat((long) lines.size()).isEqualTo(8L);
        assertThat(lines.get(0)).startsWith("order_id,customer_name");
    }
}
