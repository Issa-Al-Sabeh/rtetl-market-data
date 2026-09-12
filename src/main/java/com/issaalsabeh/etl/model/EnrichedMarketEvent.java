package com.issaalsabeh.etl.model;

import com.issaalsabeh.etl.core.IdentifiableEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record EnrichedMarketEvent (
        UUID eventId,
        String symbol,
        BigDecimal price,
        long volume,
        Instant timestamp,
        BigDecimal notionalValue
) implements IdentifiableEvent {

}
