package com.issaalsabeh.etl.core;

import com.issaalsabeh.etl.connector.console.ConsoleSink;
import com.issaalsabeh.etl.connector.console.EnrichedConsoleSink;
import com.issaalsabeh.etl.model.MarketEvent;
import com.issaalsabeh.etl.transformations.EnrichmentTransformer;
import com.issaalsabeh.etl.transformations.PriceNormalizationTransformer;
import com.issaalsabeh.etl.transformations.ValidationTransformer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineTypeValidatorTest {

    @Test
    void shouldAcceptCompatibleMarketEventPipeline() {

        Pipeline<?> pipeline =
                Pipeline.<MarketEvent>builder()
                        .source(new TestMarketSource())
                        .transform(new ValidationTransformer())
                        .transform(new PriceNormalizationTransformer())
                        .sink(new ConsoleSink())
                        .build();

        assertThatCode(() ->
                PipelineTypeValidator.validate(pipeline)
        ).doesNotThrowAnyException();
    }

    @Test
    void shouldAcceptCompatibleEnrichedMarketEventPipeline() {

        Pipeline<?> pipeline =
                Pipeline.<MarketEvent>builder()
                        .source(new TestMarketSource())
                        .transform(new ValidationTransformer())
                        .transform(new PriceNormalizationTransformer())
                        .transform(new EnrichmentTransformer())
                        .sink(new EnrichedConsoleSink())
                        .build();

        assertThatCode(() ->
                PipelineTypeValidator.validate(pipeline)
        ).doesNotThrowAnyException();
    }

    @Test
    void shouldRejectIncompatibleSinkType() {

        Pipeline<?> pipeline =
                new Pipeline<>(new TestMarketSource());

        pipeline.addTransformer(
                new EnrichmentTransformer()
        );

        pipeline.addSink(
                new ConsoleSink()
        );

        assertThatThrownBy(() ->
                PipelineTypeValidator.validate(pipeline)
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type mismatch");
    }

    @Test
    void shouldRejectIncompatibleTransformerType() {

        Pipeline<?> pipeline =
                new Pipeline<>(new TestMarketSource());

        pipeline.addTransformer(
                new EnrichmentTransformer()
        );

        pipeline.addTransformer(
                new ValidationTransformer()
        );

        pipeline.addSink(
                new EnrichedConsoleSink()
        );

        assertThatThrownBy(() ->
                PipelineTypeValidator.validate(pipeline)
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type mismatch");
    }

    private static class TestMarketSource
            implements Source<MarketEvent> {

        @Override
        public void start() {
        }

        @Override
        public MarketEvent poll() {
            return null;
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<?> getOutputType() {
            return MarketEvent.class;
        }
    }
}