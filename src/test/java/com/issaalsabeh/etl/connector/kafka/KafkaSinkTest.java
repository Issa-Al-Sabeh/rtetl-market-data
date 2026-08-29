package com.issaalsabeh.etl.connector.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KafkaSinkTest {

    @Test
    void shouldRejectNullBootstrapServers() {

        assertThatThrownBy(
                () -> new KafkaSink(
                        null,
                        "market-data-processed"
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bootstrap servers cannot be null or blank");
    }

    @Test
    void shouldRejectBlankBootstrapServers() {

        assertThatThrownBy(
                () -> new KafkaSink(
                        " ",
                        "market-data-processed"
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bootstrap servers cannot be null or blank");
    }

    @Test
    void shouldRejectNullTopic() {

        assertThatThrownBy(
                () -> new KafkaSink(
                        "localhost:9092",
                        null
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Topic cannot be null or blank");
    }

    @Test
    void shouldRejectBlankTopic() {

        assertThatThrownBy(
                () -> new KafkaSink(
                        "localhost:9092",
                        " "
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Topic cannot be null or blank");
    }
}