package com.enterprise.batch.sql;

import com.enterprise.batch.BatchApplication;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * Tests the order enrichment batch job end-to-end:
 * reads PENDING orders, transforms with tax/discount/priority,
 * writes to order_summaries table + CSV file.
 */
public class OrderEnrichmentTest {

    private int passed = 0;
    private int failed = 0;

    public static void main(String[] args) {
        new OrderEnrichmentTest().runAll();
    }

    void runAll() {
        System.out.println("=== Order Enrichment Integration Tests ===\n");

        SpringApplication app = new SpringApplication(BatchApplication.class);
        app.setDefaultProperties(Map.of("spring.batch.job.enabled", "false"));
        ConfigurableApplicationContext ctx = app.run();

        try {
            // Run the enrichment job
            JobLauncher launcher = ctx.getBean(JobLauncher.class);
            Job job = ctx.getBean("orderEnrichmentJob", Job.class);
            JobExecution exec = launcher.run(job,
                new JobParametersBuilder()
                    .addLong("run.id", System.currentTimeMillis())
                    .toJobParameters());

            StepExecution step = exec.getStepExecutions().iterator().next();

            test("Job completes successfully", () ->
                assertEquals(BatchStatus.COMPLETED, exec.getStatus()));

            test("Reads 3 PENDING orders", () ->
                assertEquals(3L, step.getReadCount()));

            test("Writes 3 enriched records", () ->
                assertEquals(3L, step.getWriteCount()));

            // --- DB verification ---
            DataSource ds = ctx.getBean(DataSource.class);

            test("order_summaries has 3 rows", () -> {
                try (Connection conn = ds.getConnection();
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT COUNT(*) FROM order_summaries")) {
                    rs.next();
                    assertEquals(3L, rs.getLong(1));
                }
            });

            // Order #4: amount=3200, customer=Acme Corp (GOLD), product=Widget C
            // tax = 3200 * 0.10 = 320.00
            // discount = 3200 * 0.15 = 480.00 (GOLD tier)
            // final = 3200 + 320 - 480 = 3040.00
            // priority = HIGH (>= 1000)
            test("Order #4 enrichment: tax=320, discount=480, final=3040, HIGH", () -> {
                try (Connection conn = ds.getConnection();
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT * FROM order_summaries WHERE order_id = 4")) {
                    assertTrue(rs.next(), "Row for order 4 should exist");
                    assertEquals("Acme Corp", rs.getString("customer_name"));
                    assertEquals("GOLD", rs.getString("customer_tier"));
                    assertEquals("Widget C", rs.getString("product_name"));
                    assertBigDecimal(new BigDecimal("3200.00"), rs.getBigDecimal("original_amount"));
                    assertBigDecimal(new BigDecimal("320.00"), rs.getBigDecimal("tax_amount"));
                    assertBigDecimal(new BigDecimal("480.00"), rs.getBigDecimal("discount_amount"));
                    assertBigDecimal(new BigDecimal("3040.00"), rs.getBigDecimal("final_amount"));
                    assertEquals("HIGH", rs.getString("priority"));
                }
            });

            // Order #1: amount=250, customer=Acme Corp (GOLD), product=Widget A
            // tax = 250 * 0.10 = 25.00
            // discount = 250 * 0.15 = 37.50
            // final = 250 + 25 - 37.50 = 237.50
            // priority = NORMAL (< 1000)
            test("Order #1 enrichment: tax=25, discount=37.50, final=237.50, NORMAL", () -> {
                try (Connection conn = ds.getConnection();
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT * FROM order_summaries WHERE order_id = 1")) {
                    assertTrue(rs.next(), "Row for order 1 should exist");
                    assertBigDecimal(new BigDecimal("25.00"), rs.getBigDecimal("tax_amount"));
                    assertBigDecimal(new BigDecimal("37.50"), rs.getBigDecimal("discount_amount"));
                    assertBigDecimal(new BigDecimal("237.50"), rs.getBigDecimal("final_amount"));
                    assertEquals("NORMAL", rs.getString("priority"));
                }
            });

            // Order #3: amount=75.50, customer=MegaStore (GOLD), product=Widget C
            // tax = 75.50 * 0.10 = 7.55
            // discount = 75.50 * 0.15 = 11.325 → stored as 11.33 (DECIMAL(10,2))
            // final = 75.50 + 7.55 - 11.325 = 71.725 → stored as 71.73
            // priority = NORMAL
            test("Order #3 enrichment: GOLD tier discount applied", () -> {
                try (Connection conn = ds.getConnection();
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT * FROM order_summaries WHERE order_id = 3")) {
                    assertTrue(rs.next(), "Row for order 3 should exist");
                    assertEquals("MegaStore", rs.getString("customer_name"));
                    assertEquals("GOLD", rs.getString("customer_tier"));
                    assertBigDecimal(new BigDecimal("7.55"), rs.getBigDecimal("tax_amount"));
                    assertEquals("NORMAL", rs.getString("priority"));
                }
            });

            // --- CSV verification ---
            Path csvPath = Path.of("output/order_summary.csv");

            test("CSV file exists", () ->
                assertTrue(Files.exists(csvPath), "output/order_summary.csv should exist"));

            test("CSV has header + 3 data rows", () -> {
                List<String> lines = Files.readAllLines(csvPath);
                assertEquals(4L, (long) lines.size());
                assertTrue(lines.get(0).startsWith("order_id,customer_name"),
                    "First line should be header");
            });

            test("CSV contains order #4 data", () -> {
                List<String> lines = Files.readAllLines(csvPath);
                boolean found = lines.stream()
                    .anyMatch(l -> l.startsWith("4,Acme Corp,GOLD,Widget C,3200"));
                assertTrue(found, "CSV should contain order #4 row");
            });

        } catch (Exception e) {
            System.out.println("  \u2717 FATAL: " + e.getMessage());
            e.printStackTrace();
            failed++;
        } finally {
            ctx.close();
            // Clean up CSV
            try { Files.deleteIfExists(Path.of("output/order_summary.csv")); } catch (Exception ignored) {}
            try { Files.deleteIfExists(Path.of("output")); } catch (Exception ignored) {}
        }

        System.out.println("\n=== Order Enrichment Results: " + passed + " passed, " + failed + " failed ===");
        if (failed > 0) System.exit(1);
    }

    // ==================== Helpers ====================

    @FunctionalInterface
    interface TestCase { void run() throws Exception; }

    private void test(String name, TestCase test) {
        try {
            test.run();
            System.out.println("  \u2713 " + name);
            passed++;
        } catch (AssertionError | Exception e) {
            System.out.println("  \u2717 " + name + " -> " + e.getMessage());
            failed++;
        }
    }

    private void assertEquals(Object expected, Object actual) {
        if (!expected.equals(actual))
            throw new AssertionError("Expected " + expected + " but got " + actual);
    }

    private void assertTrue(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    private void assertBigDecimal(BigDecimal expected, BigDecimal actual) {
        if (expected.compareTo(actual) != 0)
            throw new AssertionError("Expected " + expected + " but got " + actual);
    }
}
