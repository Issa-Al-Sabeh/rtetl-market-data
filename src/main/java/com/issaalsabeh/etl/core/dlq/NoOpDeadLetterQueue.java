package com.issaalsabeh.etl.core.dlq;

public class NoOpDeadLetterQueue implements DeadLetterQueue{
    @Override
    public void start() {

    }

    @Override
    public void publish(DeadLetterRecord record) {

    }

    @Override
    public void stop() {

    }
}
