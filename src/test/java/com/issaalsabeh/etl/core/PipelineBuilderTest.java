package com.issaalsabeh.etl.core;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineBuilderTest {

    @Test
    void shouldBuildPipelineWithSourceAndSink() {

        TestSource source = new TestSource();
        TestSink sink = new TestSink();

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .sink(sink)
                .build();

        assertThat(pipeline.getSource()).isSameAs(source);
        assertThat(pipeline.getSinks()).containsExactly(sink);
    }

    @Test
    void shouldBuildPipelineWithTransformer() {

        TestSource source = new TestSource();

        Transformer<String, String> transformer =
                new UppercaseTransformer();

        TestSink sink = new TestSink();

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .transform(transformer)
                .sink(sink)
                .build();

        assertThat(pipeline.getTransformers())
                .containsExactly(transformer);
    }

    @Test
    void shouldPreserveTransformerOrder() {

        TestSource source = new TestSource();

        Transformer<String, String> first =
                new UppercaseTransformer();

        Transformer<String, String> second =
                new TrimTransformer();

        TestSink sink = new TestSink();

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .transform(first)
                .transform(second)
                .sink(sink)
                .build();

        assertThat(pipeline.getTransformers())
                .containsExactly(first, second);
    }

    @Test
    void shouldPreserveSinkOrder() {

        TestSource source = new TestSource();

        TestSink first = new TestSink();
        TestSink second = new TestSink();

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .sink(first)
                .sink(second)
                .build();

        assertThat(pipeline.getSinks())
                .containsExactly(first, second);
    }

    @Test
    void shouldRejectMissingSource() {

        TestSink sink = new TestSink();

        assertThatThrownBy(() ->
                Pipeline.<String>builder()
                        .sink(sink)
                        .build()
        )
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldRejectMissingSink() {

        TestSource source = new TestSource();

        assertThatThrownBy(() ->
                Pipeline.<String>builder()
                        .source(source)
                        .build()
        )
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldRejectNullSource() {

        assertThatThrownBy(() ->
                Pipeline.<String>builder()
                        .source(null)
        )
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectNullTransformer() {

        assertThatThrownBy(() ->
                Pipeline.<String>builder()
                        .transform(null)
        )
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectNullSink() {

        assertThatThrownBy(() ->
                Pipeline.<String>builder()
                        .sink(null)
        )
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldProtectPipelineCollectionsFromExternalModification() {

        TestSource source = new TestSource();

        Transformer<String, String> transformer =
                new UppercaseTransformer();

        TestSink sink = new TestSink();

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .transform(transformer)
                .sink(sink)
                .build();

        assertThatThrownBy(() ->
                pipeline.getTransformers().add(new TrimTransformer())
        )
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() ->
                pipeline.getSinks().add(new TestSink())
        )
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldCreateIndependentPipelineSnapshot() {

        TestSource source = new TestSource();

        Transformer<String, String> transformer =
                new UppercaseTransformer();

        TestSink sink = new TestSink();

        Pipeline.Builder<String> builder =
                Pipeline.<String>builder()
                        .source(source)
                        .transform(transformer)
                        .sink(sink);

        Pipeline<String> firstPipeline = builder.build();

        Transformer<String, String> secondTransformer =
                new TrimTransformer();

        TestSink secondSink = new TestSink();

        builder.transform(secondTransformer);
        builder.sink(secondSink);

        Pipeline<String> secondPipeline = builder.build();

        assertThat(firstPipeline.getTransformers())
                .containsExactly(transformer);

        assertThat(firstPipeline.getSinks())
                .containsExactly(sink);

        assertThat(secondPipeline.getTransformers())
                .containsExactly(
                        transformer,
                        secondTransformer
                );

        assertThat(secondPipeline.getSinks())
                .containsExactly(
                        sink,
                        secondSink
                );
    }

    @Test
    void shouldRejectIncompatiblePipelineDuringBuild() {

        TestSource source = new TestSource();

        Transformer<Integer, Integer> transformer =
                new IntegerTransformer();

        TestSink sink = new TestSink();

        assertThatThrownBy(() ->
                Pipeline.<String>builder()
                        .source(source)
                        .transform(transformer)
                        .sink(sink)
                        .build()
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Pipeline type mismatch");
    }


    /*
     * Test helpers
     */

    private static class TestSink implements Sink<String> {

        private final List<String> received = new ArrayList<>();

        @Override
        public void start() {
        }

        @Override
        public void write(String data) {
            received.add(data);
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }
    }


    private static class UppercaseTransformer
            implements Transformer<String, String> {

        @Override
        public String transform(String input) {
            return input.toUpperCase();
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    }


    private static class TrimTransformer
            implements Transformer<String, String> {

        @Override
        public String transform(String input) {
            return input.trim();
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    }


    private static class IntegerTransformer
            implements Transformer<Integer, Integer> {

        @Override
        public Integer transform(Integer input) {
            return input;
        }

        @Override
        public Class<?> getInputType() {
            return Integer.class;
        }

        @Override
        public Class<?> getOutputType() {
            return Integer.class;
        }
    }
}