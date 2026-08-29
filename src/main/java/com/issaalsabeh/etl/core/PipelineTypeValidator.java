package com.issaalsabeh.etl.core;

import java.util.List;

public class PipelineTypeValidator {

    private PipelineTypeValidator(){}

    public static void validate(Pipeline<?> pipeline){
        Class<?> currentType = pipeline.getSource().getOutputType();

        List<Transformer<?,?>> pipelineTransformers = pipeline.getTransformers();

        for (Transformer<?, ?> transformer : pipelineTransformers) {
            if (!transformer.getInputType().isAssignableFrom(currentType)){
                throw new IllegalArgumentException(
                        "Pipeline type mismatch: " +
                        transformer.getClass().getSimpleName() +
                        " expects " +
                        transformer.getInputType().getSimpleName() +
                        " but received " +
                        currentType.getSimpleName()
                );
            }
            currentType = transformer.getOutputType();
        }



        List<Sink<?>> pipelineSinks = pipeline.getSinks();

        for (Sink<?> sink : pipelineSinks) {
            if(!sink.getInputType().isAssignableFrom(currentType)){
                throw new IllegalArgumentException(
                        "Pipeline type mismatch: sink " +
                        sink.getClass().getSimpleName() +
                        " expects " +
                        sink.getInputType().getSimpleName() +
                        " but pipeline produces " +
                        currentType.getSimpleName()
                );
            }
        }

    }
}
