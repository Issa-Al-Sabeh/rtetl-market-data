package com.issaalsabeh.etl.core.factory;

import com.issaalsabeh.etl.config.SinkConfig;
import com.issaalsabeh.etl.connector.console.ConsoleSink;
import com.issaalsabeh.etl.connector.console.EnrichedConsoleSink;
import com.issaalsabeh.etl.connector.kafka.KafkaSink;
import com.issaalsabeh.etl.connector.postgres.PostgresSink;
import com.issaalsabeh.etl.connector.redis.RedisSink;
import com.issaalsabeh.etl.core.Sink;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SinkFactoryTest {

    @Test
    void shouldCreateConsoleSink() {

        SinkConfig config = new SinkConfig(
                "console",
                Map.of()
        );

        Sink<?> sink =
                SinkFactory.create(config);

        assertThat(sink)
                .isInstanceOf(ConsoleSink.class);
    }

    @Test
    void shouldRejectUnknownSinkType() {

        SinkConfig config = new SinkConfig(
                "redis",
                Map.of()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> SinkFactory.create(config)
        );
    }

    @Test
    void shouldCreatePostgresSink() {

        SinkConfig config = new SinkConfig(
                "postgres",
                Map.of(
                        "url", "jdbc:postgresql://localhost:5432/market_data",
                        "username", "test",
                        "password", "test"
                )
        );

        Sink<?> sink =
                SinkFactory.create(config);

        assertThat(sink)
                .isInstanceOf(PostgresSink.class);
    }

    @Test
    void shouldCreateKafkaSink() {

        SinkConfig config = new SinkConfig(
                "kafka",
                Map.of(
                        "bootstrap.servers", "localhost:9092",
                        "topic", "market-data-processed"
                )
        );

        Sink<?> sink =
                SinkFactory.create(config);

        assertThat(sink)
                .isInstanceOf(KafkaSink.class);

        assertThat(sink.getInputType())
                .isEqualTo(EnrichedMarketEvent.class);
    }

    @Test
    void shouldCreateEnrichedConsoleSink() {

        SinkConfig config =
                new SinkConfig(
                        "enriched-console",
                        null
                );

        Sink<?> sink =
                SinkFactory.create(config);

        assertThat(sink)
                .isInstanceOf(EnrichedConsoleSink.class);
    }

    @Test
    void shouldRejectKafkaSinkWithMissingTopic() {

        SinkConfig config = new SinkConfig(
                "kafka",
                Map.of(
                        "bootstrap.servers", "localhost:9092"
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> SinkFactory.create(config)
        );
    }

    @Test
    void shouldRejectKafkaSinkWithMissingBootstrapServers() {

        SinkConfig config = new SinkConfig(
                "kafka",
                Map.of(
                        "topic", "market-data-processed"
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> SinkFactory.create(config)
        );
    }

    @Test
    void shouldCreateRedisSink() {

        SinkConfig config =
                new SinkConfig(
                        "redis",
                        Map.of(
                                "host", "localhost",
                                "port", "6379"
                        )
                );

        Sink<?> sink =
                SinkFactory.create(config);

        assertThat(sink)
                .isInstanceOf(RedisSink.class);

        assertThat(sink.getInputType())
                .isEqualTo(
                        EnrichedMarketEvent.class
                );
    }

    @Test
    void shouldRejectRedisSinkWithMissingHost() {

        SinkConfig config =
                new SinkConfig(
                        "redis",
                        Map.of(
                                "port", "6379"
                        )
                );

        assertThrows(
                IllegalArgumentException.class,
                () -> SinkFactory.create(config)
        );
    }

    @Test
    void shouldRejectRedisSinkWithMissingPort() {

        SinkConfig config =
                new SinkConfig(
                        "redis",
                        Map.of(
                                "host", "localhost"
                        )
                );

        assertThrows(
                IllegalArgumentException.class,
                () -> SinkFactory.create(config)
        );
    }
}
