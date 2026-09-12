package com.issaalsabeh.etl.config;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineConfigValidatorTest {

    @Test
    void shouldAcceptValidPipelineConfiguration() {

        PipelineConfig config = createValidConfig();

        assertThatCode(() -> PipelineConfigValidator.validate(config))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectMissingPipelineName() {

        PipelineConfig config = createValidConfig();
        config.getPipeline().setName(null);

        assertThatThrownBy(() -> PipelineConfigValidator.validate(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Pipeline name must not be null or blank");
    }

    @Test
    void shouldRejectBlankPipelineName() {

        PipelineConfig config = createValidConfig();
        config.getPipeline().setName("   ");

        assertThatThrownBy(() -> PipelineConfigValidator.validate(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Pipeline name must not be null or blank");
    }

    private PipelineConfig createValidConfig() {

        PipelineConfig config = new PipelineConfig();

        PipelineConfig.PipelineDefinition pipeline =
                new PipelineConfig.PipelineDefinition();

        pipeline.setName("test-pipeline");

        PipelineConfig.ConnectorConfig source =
                new PipelineConfig.ConnectorConfig();

        source.setType("mock");
        source.setProperties(Map.of());

        pipeline.setSource(source);

        pipeline.setTransformations(List.of());

        PipelineConfig.ConnectorConfig sink =
                new PipelineConfig.ConnectorConfig();

        sink.setType("console");
        sink.setProperties(Map.of());

        pipeline.setSinks(List.of(sink));

        PipelineConfig.ConnectorConfig deadLetterQueue =
                new PipelineConfig.ConnectorConfig();

        deadLetterQueue.setType("kafka");
        deadLetterQueue.setProperties(Map.of(
                "bootstrap.servers", "localhost:9092",
                "topic", "test-dlq"
        ));

        pipeline.setDeadLetterQueue(deadLetterQueue);

        config.setPipeline(pipeline);

        return config;
    }
}