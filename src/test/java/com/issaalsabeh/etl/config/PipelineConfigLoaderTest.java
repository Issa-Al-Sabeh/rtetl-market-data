package com.issaalsabeh.etl.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineConfigLoaderTest {

    @Test
    void shouldLoadPipelineConfigurationFromYaml() {

        PipelineConfigLoader loader = new PipelineConfigLoader();

        PipelineConfig config =
                loader.load("pipeline-test.yaml");

        // Pipeline
        assertThat(config).isNotNull();
        assertThat(config.getPipeline()).isNotNull();

        // Source
        assertThat(config.getPipeline().getSource().getType())
                .isEqualTo("file");

        assertThat(config.getPipeline()
                .getSource()
                .getProperties()
                .get("path"))
                .isEqualTo("data/market-events.jsonl");

        // Transformations
        assertThat(config.getPipeline().getTransformations())
                .containsExactly(
                        "validate",
                        "normalize"
                );

        // Sinks
        assertThat(config.getPipeline().getSinks())
                .hasSize(1);

        assertThat(config.getPipeline()
                .getSinks()
                .get(0)
                .getType())
                .isEqualTo("console");

        assertThat(config.getPipeline()
                .getSinks()
                .get(0)
                .getProperties())
                .isEmpty();
    }

    @Test
    void shouldFailWhenEnvironmentVariableIsMissing() {

        PipelineConfigLoader loader = new PipelineConfigLoader();

        assertThatThrownBy(
                () -> loader.load("pipeline-missing-env-test.yaml")
        )
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Environment variable is not defined"
                );
    }
}