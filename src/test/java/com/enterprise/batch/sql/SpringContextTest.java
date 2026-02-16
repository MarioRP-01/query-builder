package com.enterprise.batch.sql;

import com.enterprise.batch.BatchApplication;
import com.enterprise.batch.spring.adapter.BatchReaderFactory;
import com.enterprise.batch.spring.adapter.QueryProviderRegistry;

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

import static org.assertj.core.api.Assertions.*;

/**
 * Boots full Spring context, verifies bean wiring, launches batch job end-to-end.
 */
@SpringBootTest(classes = BatchApplication.class)
@TestPropertySource(properties = "spring.batch.job.enabled=false")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SpringContextTest {

    @Autowired
    BatchReaderFactory readerFactory;

    @Autowired
    QueryProviderRegistry registry;

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    @Qualifier("processOrdersJob")
    Job processOrdersJob;

    @Autowired
    @Qualifier("orderEnrichmentJob")
    Job orderEnrichmentJob;

    @Test
    void batchReaderFactoryBeanExists() {
        assertThat(readerFactory).isNotNull();
    }

    @Test
    void queryProviderRegistryHasProviders() {
        assertThat(registry.all()).containsKey("pendingOrders");
        assertThat(registry.all()).containsKey("highValueCustomers");
        assertThat(registry.all()).containsKey("orderDetails");
        assertThat(registry.all()).hasSize(3);
    }

    @Test
    void jobBeansExist() {
        assertThat(processOrdersJob).isNotNull();
        assertThat(orderEnrichmentJob).isNotNull();
    }

    @Test
    void jobLaunchesAndReads3PendingOrders() throws Exception {
        JobExecution exec = jobLauncher.run(processOrdersJob,
            new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters());

        assertThat(exec.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        StepExecution step = exec.getStepExecutions().iterator().next();
        assertThat(step.getReadCount()).isEqualTo(3L);
    }

    @Test
    void secondRunWithDifferentParamsSucceeds() throws Exception {
        JobExecution exec = jobLauncher.run(processOrdersJob,
            new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis() + 1)
                .toJobParameters());

        assertThat(exec.getStatus()).isEqualTo(BatchStatus.COMPLETED);
    }
}
