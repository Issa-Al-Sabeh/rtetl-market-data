package com.issaalsabeh.etl.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;

public class PipelineConfigLoader {

    private final ObjectMapper objectMapper;

    public PipelineConfigLoader() {
        this.objectMapper = new ObjectMapper(new YAMLFactory());
    }

    public PipelineConfig load() {
        return load("pipeline.yaml");
    }

    public PipelineConfig load(String resourceName) {

        try (InputStream inputStream =
                     getClass().getClassLoader().getResourceAsStream(resourceName)) {

            if (inputStream == null) {
                throw new IllegalStateException(
                        resourceName + " not found"
                );
            }

            return objectMapper.readValue(
                    inputStream,
                    PipelineConfig.class
            );

        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load pipeline configuration",
                    e
            );
        }
    }
}