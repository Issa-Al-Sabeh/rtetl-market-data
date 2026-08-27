package com.issaalsabeh.etl.core.factory;

import com.issaalsabeh.etl.config.SourceConfig;
import com.issaalsabeh.etl.connector.kafka.KafkaSource;
import com.issaalsabeh.etl.connector.mock.MockMarketSource;
import com.issaalsabeh.etl.core.Source;
import com.issaalsabeh.etl.model.MarketEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SourceFactoryTest {

    @Test
    void shouldCreateMockSource() {

        SourceConfig config = new SourceConfig(
                "mock",
                Map.of()
        );

        Source<MarketEvent> source =
                SourceFactory.create(config);

        assertThat(source)
                .isInstanceOf(MockMarketSource.class);
    }

    @Test
    void shouldRejectUnknownTypes() {

        SourceConfig config = new SourceConfig(
                "redis",
                Map.of()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> SourceFactory.create(config)
        );
    }

    @Test
    void shouldCreateKafkaSource() {

        SourceConfig config = new SourceConfig(
                "kafka",
                Map.of(
                        "bootstrap.servers", "localhost:9092",
                        "topic", "market-data",
                        "group.id", "market-data-etl"
                )
        );

        Source<MarketEvent> source =
                SourceFactory.create(config);

        assertThat(source)
                .isInstanceOf(KafkaSource.class);
    }

    @Test
    void shouldCreateKafkaSourceCaseInsensitive() {

        SourceConfig config = new SourceConfig(
                "KAFKA",
                Map.of(
                        "bootstrap.servers", "localhost:9092",
                        "topic", "market-data",
                        "group.id", "market-data-etl"
                )
        );

        Source<MarketEvent> source =
                SourceFactory.create(config);

        assertThat(source)
                .isInstanceOf(KafkaSource.class);
    }

    @Test
    void shouldRejectNullConfig() {

        assertThrows(
                IllegalArgumentException.class,
                () -> SourceFactory.create(null)
        );
    }

    @Test
    void shouldRejectKafkaSourceWithMissingProperty() {

        SourceConfig config = new SourceConfig(
                "kafka",
                Map.of(
                        "bootstrap.servers", "localhost:9092",
                        "topic", "market-data"
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> SourceFactory.create(config)
        );
    }
}