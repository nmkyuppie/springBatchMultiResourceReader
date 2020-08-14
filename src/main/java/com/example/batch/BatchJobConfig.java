package com.example.batch;

import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class BatchJobConfig {

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    JobCompletionNotificationListener jobCompletionNotificationListener;

    @Autowired
    TaskExecutor taskExecutor;

    @Bean
    Job myJob() {
        return jobBuilderFactory.get("myJob").start(myStepPartitioner()).build();
    }

    @Bean
    Step myStepPartitioner() {
        return stepBuilderFactory.get("parttioner").partitioner("myStep", partitioner()).step(myStep()).gridSize(2)
                .taskExecutor(taskExecutor).build();
    }

    private Partitioner partitioner() {
        // return new SimplePartitioner();
        return new MultiResourcePartitioner("file:D:/*.csv");
    }

    @Bean
    Step myStep() {
        return stepBuilderFactory.get("myStep").<Employee, Employee>chunk(15).reader(reader(null))
                .processor(processor()).writer(writer()).taskExecutor(taskExecutor).build();
        // return stepBuilderFactory.get("myStep").tasklet(myTasklet()).build();
    }

    @Bean
    @StepScope
    FlatFileItemReader<Employee> reader(@Value("#{stepExecutionContext[fileName]}") String filename) {
        log.info("Maniiiiiiiiiiiiii {} ", filename);
        String[] header = new String[] { "id", "name", "salary" };
        FileSystemResource fileSystemResource = new FileSystemResource(filename); // "employee.csv"
        CustomFlatFileReader<Employee> customFlatFileReader = new CustomFlatFileReader<>(header, fileSystemResource,
                Employee.class);
        return customFlatFileReader;

    }

    @Bean
    ItemProcessor<Employee, Employee> processor() {
        return new ItemProcessor<Employee, Employee>() {

            @Override
            public Employee process(final Employee item) throws Exception {
                return item;
            }

        };
    }

    @Bean
    ItemWriter<Employee> writer() {
        return new ItemWriter<Employee>() {

            @Override
            public void write(final List<? extends Employee> items) throws Exception {
                log.info("{}", items.size());
            }

        };
    }

    @Bean
    Tasklet myTasklet() {
        return new MyTasklet();
    }
}