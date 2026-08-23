package com.issaalsabeh.etl.connector.mock;

import com.issaalsabeh.etl.core.Source;
import com.issaalsabeh.etl.model.MarketEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class MockMarketSource implements Source<MarketEvent> {

    private boolean running;

    private static final String[] SYMBOLS = {
            "AAPL",
            "MSFT",
            "NVDA",
            "TSLA"
    };

    @Override
    public void start() {
        running = true;
    }

    @Override
    public MarketEvent poll() {
        if (!running) {
            throw new IllegalStateException("Source is not running");
        }

        String symbol = SYMBOLS[
                ThreadLocalRandom.current().nextInt(SYMBOLS.length)
                ];

        double randomPrice =
                ThreadLocalRandom.current().nextDouble(100.0, 500.0);

        BigDecimal price = BigDecimal.valueOf(randomPrice);

        long volume =
                ThreadLocalRandom.current().nextLong(100, 100_000);

        UUID eventId = UUID.randomUUID();

        Instant timestamp = Instant.now();

        return new MarketEvent(
                eventId,
                symbol,
                price,
                volume,
                timestamp
        );
    }

    @Override
    public void stop() {
        running = false;
    }
}