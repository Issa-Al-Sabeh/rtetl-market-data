package com.issaalsabeh.etl.connector.mock;

import com.issaalsabeh.etl.core.Source;

public class StringSource implements Source<String> {

    private boolean running;

    @Override
    public void start() {
        running = true;
    }

    @Override
    public String poll() {

        if (!running) {
            throw new IllegalStateException(
                    "StringSource has not been started"
            );
        }

        return "hello";
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public Class<?> getOutputType() {
        return String.class;
    }
}