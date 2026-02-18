package com.enterprise.batch.sql;

import com.enterprise.batch.BatchApplication;

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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end test for the condition evaluation batch job.
 *
 * <p>Step 1 (tasklet): loads 2 active groups, their conditions, and aggregates.
 * Step 2 (partitioned): reads 8 elements across 2 groups, evaluates conditions,
 * writes 9 failed-condition rows.
 *
 * <p>Expected failures:
 * <pre>
 * Group 1 (avg=250): elem2→MIN_VALUE, elem3→MAX_VALUE,
 *                     elem1/2/4→ABOVE_AVG  (5 failures)
 * Group 2 (avg=225): elem6→FLOOR, elem7→CEILING,
 *                     elem6/8→ABOVE_AVG    (4 failures)
 * </pre>
 */
@SpringBootTest(classes = BatchApplication.class)
@TestPropertySource(properties = "spring.batch.job.enabled=false")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConditionEvaluationJobTest {

    @Autowired private JobLauncher jobLauncher;
    @Autowired @Qualifier("conditionEvaluationJob") private Job conditionEvaluationJob;
    @Autowired private DataSource dataSource;

    private JobExecution jobExecution;

    @BeforeAll
    void runJob() throws Exception {
        jobExecution = jobLauncher.run(conditionEvaluationJob,
            new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters());
    }

    @Test
    void jobCompletes() {
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
    }

    @Test
    void loadingStepCompletes() {
        StepExecution loading = jobExecution.getStepExecutions().stream()
            .filter(se -> se.getStepName().equals("groupLoadingStep"))
            .findFirst().orElseThrow();
        assertThat(loading.getStatus()).isEqualTo(BatchStatus.COMPLETED);
    }

    @Test
    void failedConditionsHas9Rows() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM failed_conditions")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(9L);
        }
    }

    @Test
    void group1Has5Failures() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT COUNT(*) FROM failed_conditions WHERE group_id = 1")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(5L);
        }
    }

    @Test
    void group2Has4Failures() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT COUNT(*) FROM failed_conditions WHERE group_id = 2")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(4L);
        }
    }

    @Test
    void element2FailsMinValue() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM failed_conditions " +
                 "WHERE element_id = 2 AND condition_code = 'MIN_VALUE'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal("element_value")).isEqualByComparingTo("50.00");
            assertThat(rs.getBigDecimal("threshold_value")).isEqualByComparingTo("100.00");
        }
    }

    @Test
    void element3FailsMaxValue() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM failed_conditions " +
                 "WHERE element_id = 3 AND condition_code = 'MAX_VALUE'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal("element_value")).isEqualByComparingTo("600.00");
            assertThat(rs.getBigDecimal("threshold_value")).isEqualByComparingTo("500.00");
        }
    }

    @Test
    void element7FailsCeiling() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM failed_conditions " +
                 "WHERE element_id = 7 AND condition_code = 'CEILING'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal("element_value")).isEqualByComparingTo("400.00");
            assertThat(rs.getBigDecimal("threshold_value")).isEqualByComparingTo("350.00");
        }
    }

    @Test
    void aboveAvgFailuresUseGroupAverage() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM failed_conditions " +
                 "WHERE element_id = 4 AND condition_code = 'ABOVE_AVG'")) {
            assertThat(rs.next()).isTrue();
            // group 1 avg = 250
            assertThat(rs.getBigDecimal("threshold_value")).isEqualByComparingTo("250.00");
        }
    }

    @Test
    void partitionedStepReadAndWriteCounts() {
        // Partitioned step creates sub-step-executions; sum their counts
        Collection<StepExecution> partitionSteps = jobExecution.getStepExecutions().stream()
            .filter(se -> se.getStepName().startsWith("evaluationSlaveStep"))
            .toList();
        long totalReads = partitionSteps.stream().mapToLong(StepExecution::getReadCount).sum();
        long totalWrites = partitionSteps.stream().mapToLong(StepExecution::getWriteCount).sum();

        // 8 elements read (4 per group), 9 failures written
        assertThat(totalReads).isEqualTo(9L);  // ExplodingItemReader reports exploded count
        assertThat(totalWrites).isEqualTo(9L);
    }
}
