package com.issaalsabeh.etl.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PipelineBuilderTest {

    private final Source<String> source = new Source<>() {
        @Override
        public void start() {
        }

        @Override
        public String poll() {
            return "test";
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    };

    private final Transformer<String, String> transformer1 =
            String::trim;

    private final Transformer<String, String> transformer2 =
            String::toUpperCase;

    private final Sink<String> sink1 = new Sink<>() {
        @Override
        public void start() {
        }

        @Override
        public void write(String data) {
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }
    };

    private final Sink<String> sink2 = new Sink<>() {
        @Override
        public void start() {
        }

        @Override
        public void write(String data) {
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }
    };

    @Test
    void shouldBuildPipelineWithSourceAndSink() {
        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .sink(sink1)
                .build();

        assertEquals(source, pipeline.getSource());
        assertEquals(1, pipeline.getSinks().size());
        assertEquals(sink1, pipeline.getSinks().get(0));
    }

    @Test
    void shouldBuildPipelineWithTransformer() {
        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .transform(transformer1)
                .sink(sink1)
                .build();

        assertEquals(1, pipeline.getTransformers().size());
        assertEquals(transformer1, pipeline.getTransformers().get(0));
    }

    @Test
    void shouldPreserveTransformerOrder() {
        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .transform(transformer1)
                .transform(transformer2)
                .sink(sink1)
                .build();

        assertEquals(2, pipeline.getTransformers().size());
        assertEquals(transformer1, pipeline.getTransformers().get(0));
        assertEquals(transformer2, pipeline.getTransformers().get(1));
    }

    @Test
    void shouldAllowMultipleSinks() {
        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .sink(sink1)
                .sink(sink2)
                .build();

        assertEquals(2, pipeline.getSinks().size());
        assertEquals(sink1, pipeline.getSinks().get(0));
        assertEquals(sink2, pipeline.getSinks().get(1));
    }

    @Test
    void shouldRejectMissingSource() {
        assertThrows(
                IllegalStateException.class,
                () -> Pipeline.<String>builder()
                        .sink(sink1)
                        .build()
        );
    }

    @Test
    void shouldRejectMissingSink() {
        assertThrows(
                IllegalStateException.class,
                () -> Pipeline.<String>builder()
                        .source(source)
                        .build()
        );
    }

    @Test
    void shouldRejectNullSource() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Pipeline.<String>builder()
                        .source(null)
        );
    }

    @Test
    void shouldRejectNullTransformer() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Pipeline.<String>builder()
                        .source(source)
                        .transform(null)
        );
    }

    @Test
    void shouldRejectNullSink() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Pipeline.<String>builder()
                        .source(source)
                        .sink(null)
        );
    }

    @Test
    void shouldReturnSameBuilderWhenSettingSource() {
        Pipeline.Builder<String> builder = Pipeline.builder();

        Pipeline.Builder<String> result =
                builder.source(source);

        assertSame(builder, result);
    }

    @Test
    void shouldReturnSameBuilderWhenAddingTransformer() {
        Pipeline.Builder<String> builder = Pipeline.builder();

        Pipeline.Builder<String> result =
                builder.transform(transformer1);

        assertSame(builder, result);
    }

    @Test
    void shouldReturnSameBuilderWhenAddingSink() {
        Pipeline.Builder<String> builder = Pipeline.builder();

        Pipeline.Builder<String> result =
                builder.sink(sink1);

        assertSame(builder, result);
    }

    @Test
    void shouldProtectPipelineCollectionsFromExternalModification() {
        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .transform(transformer1)
                .sink(sink1)
                .build();

        assertThrows(
                UnsupportedOperationException.class,
                () -> pipeline.getTransformers().add(transformer2)
        );

        assertThrows(
                UnsupportedOperationException.class,
                () -> pipeline.getSinks().add(sink2)
        );
    }

    @Test
    void shouldCreateIndependentPipelineSnapshot() {
        Pipeline.Builder<String> builder = Pipeline.<String>builder()
                .source(source)
                .transform(transformer1)
                .sink(sink1);

        Pipeline<String> pipeline = builder.build();

        builder.transform(transformer2);
        builder.sink(sink2);

        assertEquals(1, pipeline.getTransformers().size());
        assertEquals(transformer1, pipeline.getTransformers().get(0));

        assertEquals(1, pipeline.getSinks().size());
        assertEquals(sink1, pipeline.getSinks().get(0));
    }
}