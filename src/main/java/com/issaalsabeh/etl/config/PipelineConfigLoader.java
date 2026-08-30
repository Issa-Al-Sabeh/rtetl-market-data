package com.issaalsabeh.etl.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PipelineConfigLoader {

    private final ObjectMapper objectMapper;

    private final Dotenv dotenv;

    private static final Pattern ENV_PATTERN =
            Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)}");

    public PipelineConfigLoader() {

        this.objectMapper = new ObjectMapper(new YAMLFactory());
        this.dotenv = Dotenv.configure()
                .ignoreIfMissing()
                .load();
    }

    private String resolveEnvironmentVariables(String content) {
        Matcher matcher = ENV_PATTERN.matcher(content);
        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            String variableName = matcher.group(1);
            String value = dotenv.get(variableName);

            if (value == null) {
                throw new IllegalStateException(
                        "Environment variable is not defined: " + variableName
                );
            }

            matcher.appendReplacement(
                    result,
                    Matcher.quoteReplacement(value)
            );
        }

        matcher.appendTail(result);

        return result.toString();
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

            String yaml = new String(
                    inputStream.readAllBytes(),
                    StandardCharsets.UTF_8
            );

            String resolvedYaml = resolveEnvironmentVariables(yaml);

            return objectMapper.readValue(
                    resolvedYaml,
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