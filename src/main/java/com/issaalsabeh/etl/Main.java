package com.issaalsabeh.etl;

import com.issaalsabeh.etl.config.SourceConfig;
import com.issaalsabeh.etl.connector.postgres.PostgresSink;
import com.issaalsabeh.etl.core.Pipeline;
import com.issaalsabeh.etl.core.PipelineExecutor;
import com.issaalsabeh.etl.core.Source;
import com.issaalsabeh.etl.core.factory.SourceFactory;
import com.issaalsabeh.etl.model.MarketEvent;
import com.issaalsabeh.etl.transformations.PriceNormalizationTransformer;
import com.issaalsabeh.etl.transformations.ValidationTransformer;

import java.util.Map;

public class Main {

    public static void main(String[] args) {

        SourceConfig config = new SourceConfig(
                "file",
                Map.of(
                        "path", "data/market-events.jsonl"
                )
        );

        Source<MarketEvent> source =
                SourceFactory.create(config);

        ValidationTransformer validationTransformer =
                new ValidationTransformer();

        PriceNormalizationTransformer priceNormalizationTransformer =
                new PriceNormalizationTransformer();

        PostgresSink postgresSink = new PostgresSink(
                "jdbc:postgresql://localhost:5432/market_data",
                System.getenv("POSTGRES_USER"),
                System.getenv("POSTGRES_PASSWORD")
        );

        Pipeline<MarketEvent> pipeline =
                Pipeline.<MarketEvent>builder()
                        .source(source)
                        .transform(validationTransformer)
                        .transform(priceNormalizationTransformer)
                        .sink(postgresSink)
                        .build();

        PipelineExecutor<MarketEvent> pipelineExecutor =
                new PipelineExecutor<>(pipeline);

        Runtime.getRuntime().addShutdownHook(
                new Thread(pipelineExecutor::stop)
        );

        pipelineExecutor.start();
    }
}