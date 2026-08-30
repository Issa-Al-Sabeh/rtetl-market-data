package com.issaalsabeh.etl.config;

public final class PipelineConfigValidator {

    private PipelineConfigValidator() {
    }

    public static void validate(PipelineConfig config) {

        if (config == null) {
            throw new IllegalArgumentException(
                    "Pipeline configuration cannot be null"
            );
        }

        if (config.getPipeline() == null) {
            throw new IllegalArgumentException(
                    "Pipeline definition is required"
            );
        }

        PipelineConfig.PipelineDefinition pipeline =
                config.getPipeline();

        if (pipeline.getSource() == null) {
            throw new IllegalArgumentException(
                    "Pipeline source is required"
            );
        }

        String sourceType = pipeline.getSource().getType();

        if (sourceType == null || sourceType.isBlank()) {
            throw new IllegalArgumentException(
                    "Pipeline source type is required"
            );
        }

        if (pipeline.getTransformations() == null) {
            throw new IllegalArgumentException(
                    "Pipeline transformations cannot be null"
            );
        }

        for (String transformation : pipeline.getTransformations()) {
            if (transformation == null || transformation.isBlank()) {
                throw new IllegalArgumentException(
                        "Transformation name cannot be null or blank"
                );
            }
        }

        if (pipeline.getSinks() == null || pipeline.getSinks().isEmpty()) {
            throw new IllegalArgumentException(
                    "Pipeline must contain at least one sink"
            );
        }

        for (PipelineConfig.ConnectorConfig sink : pipeline.getSinks()) {

            if (sink == null) {
                throw new IllegalArgumentException(
                        "Pipeline sink cannot be null"
                );
            }

            if (sink.getType() == null || sink.getType().isBlank()) {
                throw new IllegalArgumentException(
                        "Pipeline sink type is required"
                );
            }
        }

        if (pipeline.getDeadLetterQueue() == null) {
            throw new IllegalArgumentException(
                    "Pipeline must contain a Dead Letter Queue"
            );
        }

        String dlqType = pipeline.getDeadLetterQueue().getType();

        if (dlqType == null || dlqType.isBlank()) {
            throw new IllegalArgumentException(
                    "Pipeline dead letter queue type is required"
            );
        }
    }
}