package com.issaalsabeh.etl.connector.console;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.issaalsabeh.etl.model.MarketEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ConsoleSinkTest {

    private ConsoleSink sink;
    private Logger logger;
    private ListAppender<ILoggingEvent> listAppender;

    @BeforeEach
    void setUp() {

        sink = new ConsoleSink();

        logger =
                (Logger) LoggerFactory.getLogger(
                        ConsoleSink.class
                );

        listAppender = new ListAppender<>();
        listAppender.start();

        logger.addAppender(listAppender);
    }

    @AfterEach
    void tearDown() {

        logger.detachAppender(listAppender);
        listAppender.stop();
    }

    @Test
    void shouldLogStartMessage() {

        sink.start();

        assertThat(listAppender.list)
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .isEqualTo("console_sink_started")
                );
    }

    @Test
    void shouldLogMarketEvent() {

        MarketEvent event =
                new MarketEvent(
                        UUID.randomUUID(),
                        "AAPL",
                        new BigDecimal("195.1234"),
                        100,
                        Instant.now()
                );

        sink.write(event);

        assertThat(listAppender.list)
                .anySatisfy(
                        logEvent -> {

                            String message =
                                    logEvent.getFormattedMessage();

                            assertThat(message)
                                    .contains("console_event")
                                    .contains("symbol=AAPL")
                                    .contains("price=195.1234")
                                    .contains("volume=100");
                        }
                );
    }

    @Test
    void shouldLogStopMessage() {

        sink.stop();

        assertThat(listAppender.list)
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .isEqualTo("console_sink_stopped")
                );
    }
}