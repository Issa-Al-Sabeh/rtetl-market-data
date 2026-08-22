package com.issaalsabeh.etl.transformations;

import com.issaalsabeh.etl.model.MarketEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ValidationTransformerTest {

    private final ValidationTransformer transformer = new ValidationTransformer();

    @Test
    void shouldReturnValidEvent() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.00"),
                100,
                Instant.now()
        );

        MarketEvent result = transformer.transform(event);

        assertEquals(event, result);
    }

    @Test
    void shouldRejectEventWithNullSymbol(){
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                null,
                new BigDecimal("150.00"),
                100,
                Instant.now()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> transformer.transform(event)
        );
    }

    @Test
    void shouldRejectEventWithBlankSymbol() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "   ",
                new BigDecimal("150.00"),
                100,
                Instant.now()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> transformer.transform(event)
        );
    }

    @Test
    void shouldRejectEventWithNullPrice() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                null,
                100,
                Instant.now()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> transformer.transform(event)
        );
    }

    @Test
    void shouldRejectEventWithNegativePrice() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("-150.00"),
                100,
                Instant.now()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> transformer.transform(event)
        );
    }

    @Test
    void shouldAcceptEventWithZeroPrice() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                BigDecimal.ZERO,
                100,
                Instant.now()
        );

        assertDoesNotThrow(
                () -> transformer.transform(event)
        );
    }

    @Test
    void shouldRejectEventWithInvalidVolume() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.00"),
                -100,
                Instant.now()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> transformer.transform(event)
        );
    }

    @Test
    void shouldAcceptEventWithZeroVolume() {
        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.00"),
                0,
                Instant.now()
        );

        assertDoesNotThrow(
                () -> transformer.transform(event)
        );
    }
}
