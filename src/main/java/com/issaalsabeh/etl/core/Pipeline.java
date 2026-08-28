package com.issaalsabeh.etl.core;

import java.util.ArrayList;
import java.util.List;

public class Pipeline<T> {

    private final Source<T> source;
    private final List<Transformer<?, ?>> transformers;
    private final List<Sink<?>> sinks;

    public Pipeline(Source<T> source) {
        if (source == null) {
            throw new IllegalArgumentException("Source cannot be null");
        }

        this.source = source;
        this.transformers = new ArrayList<>();
        this.sinks = new ArrayList<>();
    }

    public static <T> Builder<T> builder(){
        return new Builder<>();
    }

    public Source<T> getSource() {
        return source;
    }

    public Pipeline<T> addTransformer(Transformer<?, ?> transformer) {
        if (transformer == null) {
            throw new IllegalArgumentException("Transformer cannot be null");
        }

        transformers.add(transformer);
        return this;
    }

    public List<Transformer<?, ?>> getTransformers() {
        return List.copyOf(transformers);
    }


    public Pipeline<T> addSink(Sink<?> sink) {
        if (sink == null) {
            throw new IllegalArgumentException("Sink cannot be null");
        }

        sinks.add(sink);
        return this;
    }

    public List<Sink<?>> getSinks() {
        return List.copyOf(sinks);
    }

    public void validate() {

        if (sinks.isEmpty()) {
            throw new IllegalStateException("Pipeline must have at least one sink");
        }

        PipelineTypeValidator.validate(this);
    }

    public static class Builder<T> {
        private Source<T> source;
        private final List<Transformer<?, ?>> transformers = new ArrayList<>();
        private final List<Sink<?>> sinks = new ArrayList<>();

        public Builder<T> source(Source<T> source){
            if (source == null){
                throw new IllegalArgumentException("Source cannot be null");
            }

            this.source = source;
            return this;
        }

        public Builder<T> transform(Transformer<?, ?> transformer){
            if (transformer == null){
                throw new IllegalArgumentException("Transformer cannot be null");
            }

            this.transformers.add(transformer);

            return this;
        }

        public Builder<T> sink(Sink<?> sink){
            if (sink == null){
                throw new IllegalArgumentException("Sink cannot be null");
            }

            this.sinks.add(sink);

            return this;
        }

        public Pipeline<T> build(){

            if (source == null) {
                throw new IllegalStateException("Pipeline must have a source");
            }

            Pipeline<T> pipeline = new Pipeline<>(source);

            for (Transformer<?, ?> transformer : transformers) {
                pipeline.addTransformer(transformer);
            }

            for (Sink<?> sink : sinks) {
                pipeline.addSink(sink);
            }

            pipeline.validate();

            return pipeline;
        }
    }
}