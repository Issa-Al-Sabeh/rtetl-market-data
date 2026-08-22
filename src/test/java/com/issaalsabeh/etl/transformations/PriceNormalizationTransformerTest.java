package com.issaalsabeh.etl.transformations;

import com.issaalsabeh.etl.model.MarketEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PriceNormalizationTransformerTest {

    private final PriceNormalizationTransformer transformer =
            new PriceNormalizationTransformer();

    @Test
    void shouldNormalizePriceToFourDecimalPlaces() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.5"),
                100,
                Instant.now()
        );

        MarketEvent result = transformer.transform(event);

        assertEquals(
                new BigDecimal("150.5000"),
                result.price()
        );
    }

    @Test
    void shouldRoundPriceUpUsingHalfUp() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.12345"),
                100,
                Instant.now()
        );

        MarketEvent result = transformer.transform(event);

        assertEquals(
                new BigDecimal("150.1235"),
                result.price()
        );
    }

    @Test
    void shouldRoundPriceDownUsingHalfUp() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.12344"),
                100,
                Instant.now()
        );

        MarketEvent result = transformer.transform(event);

        assertEquals(
                new BigDecimal("150.1234"),
                result.price()
        );
    }

    @Test
    void shouldKeepPriceUnchangedWhenAlreadyFourDecimalPlaces() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.1234"),
                100,
                Instant.now()
        );

        MarketEvent result = transformer.transform(event);

        assertEquals(
                new BigDecimal("150.1234"),
                result.price()
        );
    }

    @Test
    void shouldPreserveOtherMarketEventFields() {
        UUID eventId = UUID.randomUUID();
        Instant timestamp = Instant.now();

        MarketEvent event = new MarketEvent(
                eventId,
                "AAPL",
                new BigDecimal("150.123456"),
                100,
                timestamp
        );

        MarketEvent result = transformer.transform(event);

        assertEquals(eventId, result.eventId());
        assertEquals("AAPL", result.symbol());
        assertEquals(100, result.volume());
        assertEquals(timestamp, result.timestamp());
    }
}