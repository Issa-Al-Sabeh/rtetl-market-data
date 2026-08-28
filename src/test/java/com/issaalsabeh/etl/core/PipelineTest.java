    package com.issaalsabeh.etl.core;

    import org.junit.jupiter.api.Test;

    import static org.junit.jupiter.api.Assertions.*;

    class PipelineTest {

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
                input -> input.trim();

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
        void shouldCreatePipelineWithSource() {
            Pipeline<String> pipeline = new Pipeline<>(source);

            assertEquals(source, pipeline.getSource());
        }

        @Test
        void shouldRejectNullSource() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new Pipeline<String>(null)
            );
        }

        @Test
        void shouldAddTransformer() {
            Pipeline<String> pipeline = new Pipeline<>(source);

            pipeline.addTransformer(transformer1);

            assertEquals(1, pipeline.getTransformers().size());
            assertEquals(transformer1, pipeline.getTransformers().get(0));
        }

        @Test
        void shouldPreserveTransformerOrder() {
            Pipeline<String> pipeline = new Pipeline<>(source);

            pipeline.addTransformer(transformer1);
            pipeline.addTransformer(transformer2);

            assertEquals(transformer1, pipeline.getTransformers().get(0));
            assertEquals(transformer2, pipeline.getTransformers().get(1));
        }

        @Test
        void shouldRejectNullTransformer() {
            Pipeline<String> pipeline = new Pipeline<>(source);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> pipeline.addTransformer(null)
            );
        }

        @Test
        void shouldAddSink() {
            Pipeline<String> pipeline = new Pipeline<>(source);

            pipeline.addSink(sink1);

            assertEquals(1, pipeline.getSinks().size());
            assertEquals(sink1, pipeline.getSinks().get(0));
        }

        @Test
        void shouldAllowMultipleSinks() {
            Pipeline<String> pipeline = new Pipeline<>(source);

            pipeline.addSink(sink1);
            pipeline.addSink(sink2);

            assertEquals(2, pipeline.getSinks().size());
            assertEquals(sink1, pipeline.getSinks().get(0));
            assertEquals(sink2, pipeline.getSinks().get(1));
        }

        @Test
        void shouldRejectNullSink() {
            Pipeline<String> pipeline = new Pipeline<>(source);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> pipeline.addSink(null)
            );
        }

        @Test
        void shouldRejectPipelineWithNoSinks() {
            Pipeline<String> pipeline = new Pipeline<>(source);

            assertThrows(
                    IllegalStateException.class,
                    pipeline::validate
            );
        }

        @Test
        void shouldValidatePipelineWithSink() {
            Pipeline<String> pipeline = new Pipeline<>(source);

            pipeline.addSink(sink1);

            assertDoesNotThrow(pipeline::validate);
        }

        @Test
        void shouldReturnSamePipelineWhenAddingTransformer() {
            Pipeline<String> pipeline = new Pipeline<>(source);

            Pipeline<String> result = pipeline.addTransformer(transformer1);

            assertSame(pipeline, result);
        }

        @Test
        void shouldReturnSamePipelineWhenAddingSink() {
            Pipeline<String> pipeline = new Pipeline<>(source);

            Pipeline<String> result = pipeline.addSink(sink1);

            assertSame(pipeline, result);
        }
    }