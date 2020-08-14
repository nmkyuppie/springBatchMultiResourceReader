package com.example.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Map;

@Component
@Slf4j
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private static final String STR = "\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n";

    @Override
    public void afterJob(JobExecution jobExecution) {
        StringBuilder logStringBuilder = new StringBuilder();

        logStringBuilder.append("Job-Parameter: \n");
        JobParameters jp = jobExecution.getJobParameters();
        for (Iterator<Map.Entry<String, JobParameter>> iter = jp.getParameters().entrySet().iterator(); iter
                .hasNext();) {
            Map.Entry<String, JobParameter> entry = iter.next();
            logStringBuilder.append("  " + entry.getKey() + "=" + entry.getValue() + "\n");
        }
        logStringBuilder.append(STR);

        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            logStringBuilder.append(STR);
            logStringBuilder.append("Step " + stepExecution.getStepName() + " \n");
            logStringBuilder.append("ReadCount: " + stepExecution.getReadCount() + "\n");
            logStringBuilder.append("WriteCount: " + stepExecution.getWriteCount() + "\n");
            logStringBuilder.append("SkipCount: " + stepExecution.getSkipCount() + "\n");
            logStringBuilder.append("Filter: " + stepExecution.getFilterCount() + "\n");
            logStringBuilder.append("Commits: " + stepExecution.getCommitCount() + "\n");
            logStringBuilder.append("Rollbacks: " + stepExecution.getRollbackCount() + "\n");
            logStringBuilder.append(STR);
        }

        logStringBuilder.append(STR);
        logStringBuilder.append("Log for " + jobExecution.getJobInstance().getJobName() + " \n");
        logStringBuilder.append("  Started     : " + jobExecution.getStartTime() + "\n");
        logStringBuilder.append("  Finished    : " + jobExecution.getEndTime() + "\n");
        logStringBuilder.append("  Exit-Code   : " + jobExecution.getExitStatus().getExitCode() + "\n");
        logStringBuilder.append("  Exit-Descr. : " + jobExecution.getExitStatus().getExitDescription() + "\n");
        logStringBuilder.append("  Status      : " + jobExecution.getStatus() + "\n");
        logStringBuilder.append(STR);

        log.info(logStringBuilder.toString());

        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("JOB FINISHED! Time to verify the results");
        }
    }
}
