package com.example.batch;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.FileSystemResource;

public class CustomFlatFileReader<T> extends FlatFileItemReader<T> {

    private FileSystemResource fileSystemResource;

    Class<T> mapperClass;

    String header[];

    public CustomFlatFileReader(String header[], FileSystemResource fileSystemResource, Class<T> mapperClass) {
        this.header = header;
        this.fileSystemResource = fileSystemResource;
        this.mapperClass = mapperClass;
        configure();
    }

    private void configure() {
        // Set input file location
        setResource(fileSystemResource);

        // Set number of lines to skips. Use it if file has header rows.
        super.setLinesToSkip(1);

        // Configure how each line will be parsed and mapped to different values
        setLineMapper(new DefaultLineMapper<T>() {
            {
                // 3 columns in each row
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(header);
                    }
                });
                // Set values in Employee class
                setFieldSetMapper(new BeanWrapperFieldSetMapper<T>() {
                    {
                        setTargetType(mapperClass);
                    }
                });
            }
        });
    }

}