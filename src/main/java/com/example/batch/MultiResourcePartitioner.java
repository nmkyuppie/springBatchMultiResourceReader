package com.example.batch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.Assert;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MultiResourcePartitioner implements Partitioner {

    private static final String DEFAULT_KEY_NAME = "fileName";

    private static final String PARTITION_KEY = "partition";

    private Resource[] resources;

    private final String keyName = DEFAULT_KEY_NAME;

    public MultiResourcePartitioner(final String locationPattern) {
        try {
            ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
            resources = resourcePatternResolver.getResources(locationPattern);
            log.info("Saniiiiiiiiiiiiii {} ", resources.length);
        } catch (final IOException e) {
            throw new RuntimeException("I/O problems when resolving the input file pattern.", e);
        }
    }

    @Override
    public Map<String, ExecutionContext> partition(final int gridSize) {
        final Map<String, ExecutionContext> map = new HashMap<String, ExecutionContext>(gridSize);
        int i = 0;
        log.info("Saniiiiiiiiiiiiii {} ", resources.length);
        for (final Resource resource : resources) {
            final ExecutionContext context = new ExecutionContext();
            Assert.state(resource.exists(), "Resource does not exist: " + resource);
            try {
                context.putString(keyName, resource.getFile().getAbsolutePath());
                log.info("Saniiiiiiiiiiiiii {} ", resource.getFile().getAbsolutePath());
            } catch (IOException e) {
                e.printStackTrace();
            }
            map.put(PARTITION_KEY + i, context);
            i++;
        }
        return map;
    }

}