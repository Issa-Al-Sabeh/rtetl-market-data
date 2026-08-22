package com.issaalsabeh.etl.transformations;

import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import com.issaalsabeh.etl.model.MarketEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EnrichmentTransformerTest {

    private final EnrichmentTransformer transformer =
            new EnrichmentTransformer();

    @Test
    void shouldCalculateNotionalValue() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.2500"),
                100,
                Instant.now()
        );

        EnrichedMarketEvent result = transformer.transform(event);

        assertEquals(
                new BigDecimal("15025.0000"),
                result.notionalValue()
        );
    }

    @Test
    void shouldReturnZeroNotionalWhenVolumeIsZero() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.2500"),
                0,
                Instant.now()
        );

        EnrichedMarketEvent result = transformer.transform(event);

        assertEquals(
                new BigDecimal("0.0000"),
                result.notionalValue()
        );
    }

    @Test
    void shouldReturnZeroNotionalWhenPriceIsZero() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("0.0000"),
                100,
                Instant.now()
        );

        EnrichedMarketEvent result = transformer.transform(event);

        assertEquals(
                new BigDecimal("0.0000"),
                result.notionalValue()
        );
    }

    @Test
    void shouldPreserveOriginalEventFields() {
        UUID eventId = UUID.randomUUID();
        Instant timestamp = Instant.now();

        MarketEvent event = new MarketEvent(
                eventId,
                "NVDA",
                new BigDecimal("120.5000"),
                50,
                timestamp
        );

        EnrichedMarketEvent result = transformer.transform(event);

        assertEquals(eventId, result.eventId());
        assertEquals("NVDA", result.symbol());
        assertEquals(new BigDecimal("120.5000"), result.price());
        assertEquals(50, result.volume());
        assertEquals(timestamp, result.timestamp());
    }
}