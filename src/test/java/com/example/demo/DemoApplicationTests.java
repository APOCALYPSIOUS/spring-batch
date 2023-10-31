package com.example.demo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils ;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.junit.jupiter.api.BeforeEach ;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

import java.nio.file.Files;
import java.nio.file.Paths;


@SpringBootTest
@SpringBatchTest
@ExtendWith(OutputCaptureExtension.class)
class DemoApplicationTests {


    @BeforeEach
    void setUp() {
        this.jobRepositoryTestUtils.removeJobExecutions();
        JdbcTestUtils.deleteFromTables(this.jdbcTemplate, "BILLING_DATA");
    }
//    @Autowired
//    private Job job;
//
//    @Autowired
//    private JobLauncher jobLauncher;
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void testJobExecution(CapturedOutput output) throws Exception {

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.file", "src/main/resources/billing-2023-01.csv")
//                .addString("file.format", "csv",false)
                .toJobParameters();
//        JobExecution jobExecution = this.jobLauncher.run(this.job, jobParameters);
        JobExecution jobExecution = this.jobLauncherTestUtils.launchJob(jobParameters);

        Assertions.assertTrue(Files.exists(Paths.get("staging","billing-2023-01.csv")));
        Assertions.assertEquals(1000, JdbcTestUtils.countRowsInTable(jdbcTemplate,"BILLING_DATA"));
    }

}
