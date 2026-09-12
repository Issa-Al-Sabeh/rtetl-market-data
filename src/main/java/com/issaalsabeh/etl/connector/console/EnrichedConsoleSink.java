package com.issaalsabeh.etl.connector.console;

import com.issaalsabeh.etl.core.Sink;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichedConsoleSink implements Sink<EnrichedMarketEvent> {

    private static final Logger logger =
            LoggerFactory.getLogger(EnrichedConsoleSink.class);

    @Override
    public void start() {

        logger.info("enriched_console_sink_started");
    }

    @Override
    public void write(EnrichedMarketEvent data) {

        logger.info(
                "enriched_console_event symbol={} price={} volume={} timestamp={} notionalValue={}",
                data.symbol(),
                data.price(),
                data.volume(),
                data.timestamp(),
                data.notionalValue()
        );
    }

    @Override
    public void stop() {

        logger.info("enriched_console_sink_stopped");
    }

    @Override
    public Class<?> getInputType() {
        return EnrichedMarketEvent.class;
    }
}