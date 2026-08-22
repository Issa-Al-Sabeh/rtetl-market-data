package com.issaalsabeh.etl.transformations;

import com.issaalsabeh.etl.core.Transformer;
import com.issaalsabeh.etl.model.MarketEvent;

public class ValidationTransformer implements Transformer<MarketEvent, MarketEvent> {
    @Override
    public MarketEvent transform(MarketEvent event) {
        if(event == null){
            throw new IllegalArgumentException("Market event cannot be null");
        }

        if(event.symbol() == null || event.symbol().isBlank()){
            throw new IllegalArgumentException("Symbol cannot be missing");
        }

        if(event.price() == null){
            throw new IllegalArgumentException("Price cannot be null");
        }

        if(event.price().signum() < 0){
            throw new IllegalArgumentException("Price cannot be negative");
        }

        if(event.volume() < 0){
            throw new IllegalArgumentException("Volume cannot be negative");
        }

        return event;
    }
}
