package com.issaalsabeh.etl.transformations;

import com.issaalsabeh.etl.core.Transformer;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import com.issaalsabeh.etl.model.MarketEvent;

import java.math.BigDecimal;

public class EnrichmentTransformer implements Transformer<MarketEvent, EnrichedMarketEvent> {
    @Override
    public EnrichedMarketEvent transform(MarketEvent input) {
        return new EnrichedMarketEvent(
                input.eventId(),
                input.symbol(),
                input.price(),
                input.volume(),
                input.timestamp(),
                input.price().multiply(BigDecimal.valueOf(input.volume()))
        );
    }
}
