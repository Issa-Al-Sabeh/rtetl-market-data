package com.issaalsabeh.etl.connector.console;

import com.issaalsabeh.etl.model.MarketEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsoleSinkTest {

    private final PrintStream originalOut = System.out;
    private ByteArrayOutputStream outputStream;
    private ConsoleSink sink;

    @BeforeEach
    void setUp() {
        outputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));

        sink = new ConsoleSink();
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
    }

    @Test
    void shouldPrintStartMessage() {
        sink.start();

        assertTrue(outputStream.toString().contains("Console sink Started"));
    }

    @Test
    void shouldPrintMarketEvent() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("195.1234"),
                100,
                Instant.now()
        );

        sink.write(event);

        String output = outputStream.toString();

        assertTrue(output.contains("AAPL"));
        assertTrue(output.contains("195.1234"));
        assertTrue(output.contains("100"));
    }

    @Test
    void shouldPrintStopMessage() {
        sink.stop();

        assertTrue(outputStream.toString().contains("Console sink Stopped"));
    }
}