package com.issaalsabeh.etl.connector.console;

import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EnrichedConsoleSinkTest {

    private final ByteArrayOutputStream outputStream =
            new ByteArrayOutputStream();

    private PrintStream originalOut;

    @BeforeEach
    void setUp() {
        originalOut = System.out;
        System.setOut(new PrintStream(outputStream));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
    }

    @Test
    void shouldPrintEnrichedMarketEvent() {

        EnrichedConsoleSink sink =
                new EnrichedConsoleSink();

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

        String output = outputStream.toString();

        assertThat(output)
                .contains("AAPL")
                .contains("150.0000")
                .contains("100")
                .contains("15000.0000");
    }
}