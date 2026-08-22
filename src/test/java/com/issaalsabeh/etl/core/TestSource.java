package com.issaalsabeh.etl.core;

import java.util.LinkedList;
import java.util.Queue;

class TestSource implements Source<String> {

    private final Queue<String> data = new LinkedList<>();

    @Override
    public void start() {
        data.add("AAPL");
        data.add("MSFT");
    }

    @Override
    public String poll() {
        return data.poll();
    }

    @Override
    public void stop() {
        data.clear();
    }
}