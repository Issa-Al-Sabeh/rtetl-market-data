package com.issaalsabeh.etl.transformations;

import com.issaalsabeh.etl.core.Transformer;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import com.issaalsabeh.etl.model.MarketEvent;

import java.math.RoundingMode;

public class PriceNormalizationTransformer implements Transformer<MarketEvent, MarketEvent> {
    @Override
    public MarketEvent transform(MarketEvent input) {

        return new MarketEvent(
                input.eventId(),
                input.symbol(),
                input.price().setScale(4, RoundingMode.HALF_UP),
                input.volume(),
                input.timestamp()
        );
    }

    @Override
    public Class<?> getInputType() {
        return MarketEvent.class;
    }

    @Override
    public Class<?> getOutputType() {
        return MarketEvent.class;
    }
}
