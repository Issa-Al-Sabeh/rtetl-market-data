package com.issaalsabeh.etl.core.factory;

import com.issaalsabeh.etl.config.SourceConfig;
import com.issaalsabeh.etl.connector.file.FileSource;
import com.issaalsabeh.etl.connector.kafka.KafkaSource;
import com.issaalsabeh.etl.connector.mock.MockMarketSource;
import com.issaalsabeh.etl.core.Source;
import com.issaalsabeh.etl.model.MarketEvent;

public final class SourceFactory {

    private SourceFactory() {}

    public static Source<MarketEvent> create(SourceConfig config) {

        if (config == null) {
            throw new IllegalArgumentException(
                    "Source config cannot be null"
            );
        }

        String type = config.getType();

        if (type.equalsIgnoreCase("mock")) {
            return new MockMarketSource();
        }

        if (type.equalsIgnoreCase("file")) {

            String path = config.getProperty("path");

            return new FileSource(path);
        }

        if (type.equalsIgnoreCase("kafka")) {

            String bootstrapServers =
                    config.getProperty("bootstrap.servers");

            String topic =
                    config.getProperty("topic");

            String groupId =
                    config.getProperty("group.id");

            return new KafkaSource(
                    bootstrapServers,
                    topic,
                    groupId
            );
        }

        throw new IllegalArgumentException(
                "Unsupported source type: " + type
        );
    }
}