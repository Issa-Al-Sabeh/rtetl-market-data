package com.issaalsabeh.etl.core.factory;

import com.issaalsabeh.etl.config.PipelineConfig;
import com.issaalsabeh.etl.config.PipelineConfigLoader;
import com.issaalsabeh.etl.connector.console.ConsoleSink;
import com.issaalsabeh.etl.connector.console.EnrichedConsoleSink;
import com.issaalsabeh.etl.connector.file.FileSource;
import com.issaalsabeh.etl.connector.kafka.KafkaSink;
import com.issaalsabeh.etl.connector.kafka.KafkaSource;
import com.issaalsabeh.etl.connector.postgres.PostgresSink;
import com.issaalsabeh.etl.connector.redis.RedisSink;
import com.issaalsabeh.etl.core.Pipeline;
import com.issaalsabeh.etl.core.Sink;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import com.issaalsabeh.etl.model.MarketEvent;
import com.issaalsabeh.etl.transformations.EnrichmentTransformer;
import com.issaalsabeh.etl.transformations.PriceNormalizationTransformer;
import com.issaalsabeh.etl.transformations.ValidationTransformer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineFactoryTest {

    @Test
    void shouldBuildValidConfiguredPipeline() {

        PipelineConfigLoader loader =
                new PipelineConfigLoader();

        PipelineConfig config =
                loader.load("pipeline-test.yaml");

        Pipeline<?> pipeline =
                PipelineFactory.create(config);

        // Source
        assertThat(pipeline.getSource())
                .isInstanceOf(FileSource.class);

        assertThat(pipeline.getSource().getOutputType())
                .isEqualTo(MarketEvent.class);

        // Transformers
        assertThat(pipeline.getTransformers())
                .hasSize(2);

        assertThat(pipeline.getTransformers().get(0))
                .isInstanceOf(ValidationTransformer.class);

        assertThat(pipeline.getTransformers().get(0).getInputType())
                .isEqualTo(MarketEvent.class);

        assertThat(pipeline.getTransformers().get(0).getOutputType())
                .isEqualTo(MarketEvent.class);

        assertThat(pipeline.getTransformers().get(1))
                .isInstanceOf(PriceNormalizationTransformer.class);

        assertThat(pipeline.getTransformers().get(1).getInputType())
                .isEqualTo(MarketEvent.class);

        assertThat(pipeline.getTransformers().get(1).getOutputType())
                .isEqualTo(MarketEvent.class);

        // Sink
        assertThat(pipeline.getSinks())
                .hasSize(1);

        assertThat(pipeline.getSinks().get(0))
                .isInstanceOf(ConsoleSink.class);

        assertThat(pipeline.getSinks().get(0).getInputType())
                .isEqualTo(MarketEvent.class);
    }

    @Test
    void shouldRejectInvalidConfiguredTypeChain() {

        PipelineConfigLoader loader =
                new PipelineConfigLoader();

        PipelineConfig config =
                loader.load("pipeline-invalid-test.yaml");

        assertThatThrownBy(() ->
                PipelineFactory.create(config)
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type mismatch");
    }

    @Test
    void shouldBuildProductionPipelineConfiguration() {

        PipelineConfigLoader loader =
                new PipelineConfigLoader();

        PipelineConfig config =
                loader.load("pipeline.yaml");

        Pipeline<?> pipeline =
                PipelineFactory.create(config);

        try {
            assertThat(pipeline.getSource())
                    .isInstanceOf(KafkaSource.class);

            assertThat(pipeline.getSource().getOutputType())
                    .isEqualTo(MarketEvent.class);

            assertThat(pipeline.getTransformers())
                    .hasSize(3);

            assertThat(pipeline.getTransformers().get(0))
                    .isInstanceOf(ValidationTransformer.class);

            assertThat(pipeline.getTransformers().get(1))
                    .isInstanceOf(PriceNormalizationTransformer.class);

            assertThat(pipeline.getTransformers().get(2))
                    .isInstanceOf(EnrichmentTransformer.class);

            assertThat(pipeline.getTransformers().get(2).getOutputType())
                    .isEqualTo(EnrichedMarketEvent.class);

            assertThat(pipeline.getSinks())
                    .hasSize(3);

            assertThat(pipeline.getSinks().get(0))
                    .isInstanceOf(KafkaSink.class);

            assertThat(pipeline.getSinks().get(0).getInputType())
                    .isEqualTo(EnrichedMarketEvent.class);

            assertThat(pipeline.getSinks().get(1))
                    .isInstanceOf(RedisSink.class);

            assertThat(pipeline.getSinks().get(1).getInputType())
                    .isEqualTo(EnrichedMarketEvent.class);

            assertThat(pipeline.getSinks().get(2))
                    .isInstanceOf(PostgresSink.class);

            assertThat(pipeline.getSinks().get(2).getInputType())
                    .isEqualTo(EnrichedMarketEvent.class);

        } finally {
            pipeline.getSource().stop();

            for (Sink<?> sink : pipeline.getSinks()) {
                sink.stop();
            }
        }
    }
}