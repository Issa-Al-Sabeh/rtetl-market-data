package com.issaalsabeh.etl.connector.console;

import com.issaalsabeh.etl.core.Sink;
import com.issaalsabeh.etl.model.MarketEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleSink implements Sink<MarketEvent> {

    private static final Logger logger =
            LoggerFactory.getLogger(ConsoleSink.class);

    @Override
    public void start() {

        logger.info("console_sink_started");
    }

    @Override
    public void write(MarketEvent data) {

        logger.info(
                "console_event symbol={} price={} volume={} timestamp={}",
                data.symbol(),
                data.price(),
                data.volume(),
                data.timestamp()
        );
    }

    @Override
    public void stop() {

        logger.info("console_sink_stopped");
    }

    @Override
    public Class<?> getInputType() {
        return MarketEvent.class;
    }
}