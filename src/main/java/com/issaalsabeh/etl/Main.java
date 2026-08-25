package com.issaalsabeh.etl;

import com.issaalsabeh.etl.connector.console.ConsoleSink;
import com.issaalsabeh.etl.connector.kafka.KafkaSource;
import com.issaalsabeh.etl.connector.mock.MockMarketSource;
import com.issaalsabeh.etl.connector.postgres.PostgresSink;
import com.issaalsabeh.etl.core.Pipeline;
import com.issaalsabeh.etl.core.PipelineExecutor;
import com.issaalsabeh.etl.model.MarketEvent;
import com.issaalsabeh.etl.transformations.PriceNormalizationTransformer;
import com.issaalsabeh.etl.transformations.ValidationTransformer;

public class Main {

    public static void main(String[] args) {
//        MockMarketSource source = new MockMarketSource();
        KafkaSource source = new KafkaSource();
        ValidationTransformer validationTransformer = new ValidationTransformer();
        PriceNormalizationTransformer priceNormalizationTransformer = new PriceNormalizationTransformer();
//        ConsoleSink consoleSink = new ConsoleSink();
        PostgresSink postgresSink = new PostgresSink(
                "jdbc:postgresql://localhost:5432/market_data",
                System.getenv("POSTGRES_USER"),
                System.getenv("POSTGRES_PASSWORD")
        );

        Pipeline<MarketEvent> pipeline = Pipeline.<MarketEvent>builder()
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