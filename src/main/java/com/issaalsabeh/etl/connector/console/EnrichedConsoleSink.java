package com.issaalsabeh.etl.connector.console;

import com.issaalsabeh.etl.core.Sink;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import com.issaalsabeh.etl.model.MarketEvent;

public class EnrichedConsoleSink implements Sink<EnrichedMarketEvent> {
    @Override
    public void start() {
        System.out.println("Enriched Console sink Started");
    }

    @Override
    public void write(EnrichedMarketEvent data) {
        System.out.println(data);
    }

    @Override
    public void stop() {
        System.out.println("Enriched Console sink Stopped");
    }

    @Override
    public Class<?> getInputType() {
        return EnrichedMarketEvent.class;
    }
}
