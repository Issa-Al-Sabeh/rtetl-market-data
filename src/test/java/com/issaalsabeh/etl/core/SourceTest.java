package com.issaalsabeh.etl.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SourceTest {

    @Test
    void shouldPollDataFromSource() {

        Source<String> source = new TestSource();

        source.start();

        assertEquals("AAPL", source.poll());
        assertEquals("MSFT", source.poll());

        source.stop();
    }
}
