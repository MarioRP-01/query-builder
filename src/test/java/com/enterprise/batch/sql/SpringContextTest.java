package com.enterprise.batch.sql;

import com.enterprise.batch.BatchApplication;
import com.enterprise.batch.spring.BatchReaderFactory;
import com.enterprise.batch.spring.QueryProviderRegistry;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Map;

/**
 * Boots full Spring context, verifies bean wiring, launches batch job end-to-end.
 * Standalone test (no JUnit) — matches project convention.
 */
public class SpringContextTest {

    private int passed = 0;
    private int failed = 0;

    public static void main(String[] args) {
        new SpringContextTest().runAll();
    }

    void runAll() {
        System.out.println("=== Spring Context Integration Tests ===\n");

        SpringApplication app = new SpringApplication(BatchApplication.class);
        app.setDefaultProperties(Map.of("spring.batch.job.enabled", "false"));
        ConfigurableApplicationContext ctx = app.run();

        try {
            test("Context boots successfully", () ->
                assertTrue(ctx.isActive(), "Context should be active"));

            test("BatchReaderFactory bean exists", () ->
                assertNotNull(ctx.getBean(BatchReaderFactory.class)));

            test("QueryProviderRegistry has providers", () -> {
                QueryProviderRegistry reg = ctx.getBean(QueryProviderRegistry.class);
                assertTrue(reg.all().containsKey("pendingOrders"),
                    "Missing pendingOrders provider");
                assertTrue(reg.all().containsKey("highValueCustomers"),
                    "Missing highValueCustomers provider");
                assertTrue(reg.all().containsKey("orderDetails"),
                    "Missing orderDetails provider");
                assertEquals(3, reg.all().size());
            });

            test("Job beans exist", () -> {
                assertNotNull(ctx.getBean("processOrdersJob", Job.class));
                assertNotNull(ctx.getBean("orderEnrichmentJob", Job.class));
            });

            test("Job launches and reads 3 PENDING orders", () -> {
                JobLauncher launcher = ctx.getBean(JobLauncher.class);
                Job job = ctx.getBean("processOrdersJob", Job.class);
                JobExecution exec = launcher.run(job,
                    new JobParametersBuilder()
                        .addLong("run.id", System.currentTimeMillis())
                        .toJobParameters());

                assertEquals(BatchStatus.COMPLETED, exec.getStatus());
                StepExecution step = exec.getStepExecutions().iterator().next();
                assertEquals(3L, step.getReadCount());
            });

            test("Second run with different params succeeds", () -> {
                JobLauncher launcher = ctx.getBean(JobLauncher.class);
                Job job = ctx.getBean("processOrdersJob", Job.class);
                JobExecution exec = launcher.run(job,
                    new JobParametersBuilder()
                        .addLong("run.id", System.currentTimeMillis() + 1)
                        .toJobParameters());

                assertEquals(BatchStatus.COMPLETED, exec.getStatus());
            });
        } catch (Exception e) {
            System.out.println("  \u2717 FATAL: " + e.getMessage());
            failed++;
        } finally {
            ctx.close();
        }

        System.out.println("\n=== Spring Context Results: " + passed + " passed, " + failed + " failed ===");
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

    private void assertNotNull(Object obj) {
        if (obj == null) throw new AssertionError("Expected non-null");
    }
}
