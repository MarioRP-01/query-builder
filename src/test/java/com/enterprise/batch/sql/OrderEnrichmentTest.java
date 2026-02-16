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
 * Tests the order enrichment batch job end-to-end:
 * reads PENDING orders, transforms with tax/discount/priority,
 * writes to order_summaries table + CSV file.
 */
@SpringBootTest(classes = BatchApplication.class)
@TestPropertySource(properties = "spring.batch.job.enabled=false")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OrderEnrichmentTest {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("orderEnrichmentJob")
    private Job orderEnrichmentJob;

    @Autowired
    private DataSource dataSource;

    private JobExecution jobExecution;
    private StepExecution stepExecution;

    @BeforeAll
    void runJob() throws Exception {
        jobExecution = jobLauncher.run(orderEnrichmentJob,
            new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters());
        stepExecution = jobExecution.getStepExecutions().iterator().next();
    }

    @AfterAll
    void cleanup() {
        try { Files.deleteIfExists(Path.of("output/order_summary.csv")); } catch (Exception ignored) {}
        try { Files.deleteIfExists(Path.of("output")); } catch (Exception ignored) {}
    }

    @Test
    void jobCompletesSuccessfully() {
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
    }

    @Test
    void reads3PendingOrders() {
        assertThat(stepExecution.getReadCount()).isEqualTo(3L);
    }

    @Test
    void writes3EnrichedRecords() {
        assertThat(stepExecution.getWriteCount()).isEqualTo(3L);
    }

    @Test
    void orderSummariesHas3Rows() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT COUNT(*) FROM order_summaries")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(3L);
        }
    }

    // Order #4: amount=3200, customer=Acme Corp (GOLD), product=Widget C
    // tax = 3200 * 0.10 = 320.00
    // discount = 3200 * 0.15 = 480.00 (GOLD tier)
    // final = 3200 + 320 - 480 = 3040.00
    // priority = HIGH (>= 1000)
    @Test
    void order4Enrichment() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM order_summaries WHERE order_id = 4")) {
            assertThat(rs.next()).as("Row for order 4 should exist").isTrue();
            assertThat(rs.getString("customer_name")).isEqualTo("Acme Corp");
            assertThat(rs.getString("customer_tier")).isEqualTo("GOLD");
            assertThat(rs.getString("product_name")).isEqualTo("Widget C");
            assertThat(rs.getBigDecimal("original_amount")).isEqualByComparingTo(new BigDecimal("3200.00"));
            assertThat(rs.getBigDecimal("tax_amount")).isEqualByComparingTo(new BigDecimal("320.00"));
            assertThat(rs.getBigDecimal("discount_amount")).isEqualByComparingTo(new BigDecimal("480.00"));
            assertThat(rs.getBigDecimal("final_amount")).isEqualByComparingTo(new BigDecimal("3040.00"));
            assertThat(rs.getString("priority")).isEqualTo("HIGH");
        }
    }

    // Order #1: amount=250, customer=Acme Corp (GOLD), product=Widget A
    // tax = 250 * 0.10 = 25.00
    // discount = 250 * 0.15 = 37.50
    // final = 250 + 25 - 37.50 = 237.50
    // priority = NORMAL (< 1000)
    @Test
    void order1Enrichment() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM order_summaries WHERE order_id = 1")) {
            assertThat(rs.next()).as("Row for order 1 should exist").isTrue();
            assertThat(rs.getBigDecimal("tax_amount")).isEqualByComparingTo(new BigDecimal("25.00"));
            assertThat(rs.getBigDecimal("discount_amount")).isEqualByComparingTo(new BigDecimal("37.50"));
            assertThat(rs.getBigDecimal("final_amount")).isEqualByComparingTo(new BigDecimal("237.50"));
            assertThat(rs.getString("priority")).isEqualTo("NORMAL");
        }
    }

    // Order #3: amount=75.50, customer=MegaStore (GOLD), product=Widget C
    // tax = 75.50 * 0.10 = 7.55
    // discount = 75.50 * 0.15 = 11.325 → stored as 11.33 (DECIMAL(10,2))
    // final = 75.50 + 7.55 - 11.325 = 71.725 → stored as 71.73
    // priority = NORMAL
    @Test
    void order3Enrichment() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM order_summaries WHERE order_id = 3")) {
            assertThat(rs.next()).as("Row for order 3 should exist").isTrue();
            assertThat(rs.getString("customer_name")).isEqualTo("MegaStore");
            assertThat(rs.getString("customer_tier")).isEqualTo("GOLD");
            assertThat(rs.getBigDecimal("tax_amount")).isEqualByComparingTo(new BigDecimal("7.55"));
            assertThat(rs.getString("priority")).isEqualTo("NORMAL");
        }
    }

    @Test
    void csvFileExists() {
        Path csvPath = Path.of("output/order_summary.csv");
        assertThat(Files.exists(csvPath)).as("output/order_summary.csv should exist").isTrue();
    }

    @Test
    void csvHasHeaderAnd3DataRows() throws Exception {
        List<String> lines = Files.readAllLines(Path.of("output/order_summary.csv"));
        assertThat((long) lines.size()).isEqualTo(4L);
        assertThat(lines.get(0).startsWith("order_id,customer_name"))
            .as("First line should be header").isTrue();
    }

    @Test
    void csvContainsOrder4Data() throws Exception {
        List<String> lines = Files.readAllLines(Path.of("output/order_summary.csv"));
        boolean found = lines.stream()
            .anyMatch(l -> l.startsWith("4,Acme Corp,GOLD,Widget C,3200"));
        assertThat(found).as("CSV should contain order #4 row").isTrue();
    }
}
