package com.issaalsabeh.etl.connector.mock;

import com.issaalsabeh.etl.model.MarketEvent;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MockMarketSourceTest {

    @Test
    void shouldGenerateMarketEvent() {
        MockMarketSource source = new MockMarketSource();

        source.start();

        MarketEvent event = source.poll();

        assertNotNull(event);
        assertNotNull(event.eventId());
        assertNotNull(event.symbol());
        assertNotNull(event.price());
        assertTrue(event.volume() > 0);
        assertNotNull(event.timestamp());

        assertTrue(
                List.of("AAPL", "MSFT", "NVDA", "TSLA")
                        .contains(event.symbol())
        );

        source.stop();
    }

    @Test
    void shouldGenerateDifferentEventIds() {
        MockMarketSource source = new MockMarketSource();

        source.start();

        MarketEvent firstEvent = source.poll();
        MarketEvent secondEvent = source.poll();

        assertNotEquals(
                firstEvent.eventId(),
                secondEvent.eventId()
        );

        source.stop();
    }
    @Test
    void shouldThrowExceptionWhenPollingBeforeStart() {
        MockMarketSource source = new MockMarketSource();

        assertThrows(
                IllegalStateException.class,
                source::poll
        );
    }

}