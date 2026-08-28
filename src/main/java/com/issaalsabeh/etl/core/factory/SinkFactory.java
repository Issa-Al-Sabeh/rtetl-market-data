package com.issaalsabeh.etl.core.factory;

import com.issaalsabeh.etl.config.SinkConfig;
import com.issaalsabeh.etl.connector.console.ConsoleSink;
import com.issaalsabeh.etl.connector.console.EnrichedConsoleSink;
import com.issaalsabeh.etl.connector.kafka.KafkaSink;
import com.issaalsabeh.etl.connector.postgres.PostgresSink;
import com.issaalsabeh.etl.core.Sink;

public final class SinkFactory {

    private SinkFactory() {
    }

    public static Sink<?> create(SinkConfig config) {

        if (config == null) {
            throw new IllegalArgumentException(
                    "Sink config cannot be null"
            );
        }

        String type = config.getType();

        if (type.equalsIgnoreCase("console")) {
            return new ConsoleSink();
        }

        if (type.equalsIgnoreCase("enriched-console")) {
            return new EnrichedConsoleSink();
        }

        if (type.equalsIgnoreCase("kafka")) {
            String bootstrapServers = config.getProperty("bootstrap.servers");
            String topic = config.getProperty("topic");

            return new KafkaSink(
                    bootstrapServers,
                    topic
            );
        }

        if (type.equalsIgnoreCase("postgres")) {

            String url = config.getProperty("url");
            String username = config.getProperty("username");
            String password = config.getProperty("password");

            return new PostgresSink(
                    url,
                    username,
                    password
            );
        }

        throw new IllegalArgumentException(
                "Unsupported sink type: " + type
        );
    }
}