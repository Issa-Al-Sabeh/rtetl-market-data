package com.issaalsabeh.etl.core.factory;

import com.issaalsabeh.etl.config.PipelineConfig;
import com.issaalsabeh.etl.config.PipelineConfigValidator;
import com.issaalsabeh.etl.config.SinkConfig;
import com.issaalsabeh.etl.config.SourceConfig;
import com.issaalsabeh.etl.core.*;

import java.util.ArrayList;
import java.util.List;

public final class PipelineFactory {

    private PipelineFactory() {
    }

    public static Pipeline<?> create(PipelineConfig config){

        PipelineConfigValidator.validate(config);

        Source<?> source = createSource(config);
        List<Transformer<?, ?>> transformers = createTransformers(config);
        List<Sink<?>> sinks = createSinks(config);

        return buildPipeline(
                source,
                transformers,
                sinks
        );

    }

    private static Source<?> createSource(PipelineConfig config) {

        PipelineConfig.ConnectorConfig yamlSource =
                config.getPipeline().getSource();

        SourceConfig sourceConfig =
                new SourceConfig(
                        yamlSource.getType(),
                        yamlSource.getProperties()
                );

        return SourceFactory.create(sourceConfig);
    }

    private static List<Transformer<?, ?>> createTransformers(PipelineConfig config) {

        List<String> transformationNames = config.getPipeline().getTransformations();

        List<Transformer<?, ?>> transformers = new ArrayList<>();

        for (String transformationName : transformationNames) {
            transformers.add(
                    TransformerFactory.create(transformationName)
            );
        }

        return transformers;
    }

    private static List<Sink<?>> createSinks(PipelineConfig config) {

        List<PipelineConfig.ConnectorConfig> sinkConfigurations =
                config.getPipeline().getSinks();

        List<Sink<?>> sinks = new ArrayList<>();

        for (PipelineConfig.ConnectorConfig sinkConfiguration : sinkConfigurations) {

            SinkConfig sinkConfig =
                    new SinkConfig(
                            sinkConfiguration.getType(),
                            sinkConfiguration.getProperties()
                    );

            sinks.add(
                    SinkFactory.create(sinkConfig)
            );
        }

        return sinks;
    }

    private static <T> Pipeline<T> buildPipeline(
            Source<T> source,
            List<Transformer<?, ?>> transformers,
            List<Sink<?>> sinks) {

        Pipeline.Builder<T> builder =
                Pipeline.<T>builder()
                        .source(source);

        for (Transformer<?, ?> transformer : transformers) {
            builder.transform(transformer);
        }

        for (Sink<?> sink : sinks) {
            builder.sink(sink);
        }

        return builder.build();
    }
}