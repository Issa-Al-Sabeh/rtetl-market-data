package com.issaalsabeh.etl.connector.console;

import com.issaalsabeh.etl.core.Sink;
import com.issaalsabeh.etl.model.MarketEvent;

public class ConsoleSink implements Sink<MarketEvent> {
    @Override
    public void start() {
        System.out.println("Console sink Started");
    }

    @Override
    public void write(MarketEvent data) {
        System.out.println(data);
    }

    @Override
    public void stop() {
        System.out.println("Console sink Stopped");
    }
}
