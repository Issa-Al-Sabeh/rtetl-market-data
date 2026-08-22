package com.issaalsabeh.etl.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record MarketEvent(
        UUID eventId,
        String symbol,
        BigDecimal price,
        long volume,
        Instant timestamp
) {}
