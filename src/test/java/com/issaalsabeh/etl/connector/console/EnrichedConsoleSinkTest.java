package com.issaalsabeh.etl.connector.console;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EnrichedConsoleSinkTest {

    private EnrichedConsoleSink sink;
    private Logger logger;
    private ListAppender<ILoggingEvent> listAppender;

    @BeforeEach
    void setUp() {

        sink = new EnrichedConsoleSink();

        logger =
                (Logger) LoggerFactory.getLogger(
                        EnrichedConsoleSink.class
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
    void shouldLogEnrichedMarketEvent() {

        EnrichedMarketEvent event =
                new EnrichedMarketEvent(
                        UUID.randomUUID(),
                        "AAPL",
                        new BigDecimal("150.0000"),
                        100,
                        Instant.parse("2026-08-28T12:00:00Z"),
                        new BigDecimal("15000.0000")
                );

        sink.write(event);

        assertThat(listAppender.list)
                .anySatisfy(
                        logEvent -> {

                            String message =
                                    logEvent.getFormattedMessage();

                            assertThat(message)
                                    .contains("enriched_console_event")
                                    .contains("symbol=AAPL")
                                    .contains("price=150.0000")
                                    .contains("volume=100")
                                    .contains("notionalValue=15000.0000");
                        }
                );
    }

    @Test
    void shouldLogStartMessage() {

        sink.start();

        assertThat(listAppender.list)
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .isEqualTo(
                                                "enriched_console_sink_started"
                                        )
                );
    }

    @Test
    void shouldLogStopMessage() {

        sink.stop();

        assertThat(listAppender.list)
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .isEqualTo(
                                                "enriched_console_sink_stopped"
                                        )
                );
    }
}