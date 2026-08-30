package com.issaalsabeh.etl.connector.redis;

import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RedisSinkTest {

    @Test
    void shouldRejectNullHost() {

        assertThatThrownBy(
                () -> new RedisSink(
                        null,
                        6379
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Redis host cannot be null or blank");
    }

    @Test
    void shouldRejectBlankHost() {

        assertThatThrownBy(
                () -> new RedisSink(
                        " ",
                        6379
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Redis host cannot be null or blank");
    }

    @Test
    void shouldRejectZeroPort() {

        assertThatThrownBy(
                () -> new RedisSink(
                        "localhost",
                        0
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Redis port must be greater than zero");
    }

    @Test
    void shouldRejectNegativePort() {

        assertThatThrownBy(
                () -> new RedisSink(
                        "localhost",
                        -1
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Redis port must be greater than zero");
    }

    @Test
    void shouldThrowWhenWritingBeforeStart() {

        RedisSink sink =
                new RedisSink(
                        "localhost",
                        6379
                );

        EnrichedMarketEvent event =
                new EnrichedMarketEvent(
                        UUID.randomUUID(),
                        "AAPL",
                        new BigDecimal("150.2500"),
                        100,
                        Instant.now(),
                        new BigDecimal("15025.0000")
                );

        assertThatThrownBy(
                () -> sink.write(event)
        )
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Sink is not running");
    }

    @Test
    void shouldReportEnrichedMarketEventInputType() {

        RedisSink sink =
                new RedisSink(
                        "localhost",
                        6379
                );

        assertThat(sink.getInputType())
                .isEqualTo(EnrichedMarketEvent.class);
    }
}