package com.issaalsabeh.etl;

import com.issaalsabeh.etl.config.SourceConfig;
import com.issaalsabeh.etl.connector.console.ConsoleSink;
import com.issaalsabeh.etl.core.Pipeline;
import com.issaalsabeh.etl.core.PipelineExecutor;
import com.issaalsabeh.etl.core.Source;
import com.issaalsabeh.etl.core.factory.SourceFactory;
import com.issaalsabeh.etl.transformations.PriceNormalizationTransformer;
import com.issaalsabeh.etl.transformations.ValidationTransformer;

import java.util.Map;

public class Main2 {

    public static void main(String[] args) {

        SourceConfig sourceConfig =
                new SourceConfig(
                        "file",
                        Map.of(
                                "path",
                                "data/market-events.jsonl"
                        )
                );

        Source<?> source =
                SourceFactory.create(sourceConfig);

        Pipeline<?> pipeline =
                buildPipeline(source);

        PipelineExecutor<?> executor =
                new PipelineExecutor<>(pipeline);

        Runtime.getRuntime().addShutdownHook(
                new Thread(executor::stop)
        );

        executor.start();
    }

    private static <T> Pipeline<T> buildPipeline(
            Source<T> source) {

        return Pipeline.<T>builder()
                .source(source)
                .transform(new ValidationTransformer())
                .transform(new PriceNormalizationTransformer())
                .sink(new ConsoleSink())
                .build();
    }
}