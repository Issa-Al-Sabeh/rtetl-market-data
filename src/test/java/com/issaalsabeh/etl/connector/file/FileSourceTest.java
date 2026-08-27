package com.issaalsabeh.etl.connector.file;

import com.issaalsabeh.etl.model.MarketEvent;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileSourceTest {

    @Test
    void shouldReadMarketEventFromFile() throws Exception {

        String path = Path.of(
                getClass()
                        .getClassLoader()
                        .getResource("market-events-test.jsonl")
                        .toURI()
        ).toString();

        FileSource source = new FileSource(path);

        source.start();

        MarketEvent event = source.poll();

        assertThat(event).isNotNull();
        assertThat(event.symbol()).isEqualTo("AAPL");

        source.stop();
    }

    @Test
    void shouldReadMultipleEvents() throws Exception {

        String path = Path.of(
                getClass()
                        .getClassLoader()
                        .getResource("market-events-test.jsonl")
                        .toURI()
        ).toString();

        FileSource source = new FileSource(path);

        source.start();

        MarketEvent first = source.poll();
        MarketEvent second = source.poll();

        assertThat(first.symbol()).isEqualTo("AAPL");
        assertThat(second.symbol()).isEqualTo("MSFT");

        source.stop();
    }

    @Test
    void shouldReturnNullAtEndOfFile() throws Exception {

        String path = Path.of(
                getClass()
                        .getClassLoader()
                        .getResource("market-events-test.jsonl")
                        .toURI()
        ).toString();

        FileSource source = new FileSource(path);

        source.start();

        source.poll();
        source.poll();

        MarketEvent event = source.poll();

        assertThat(event).isNull();

        source.stop();
    }

    @Test
    void shouldThrowWhenPollingBeforeStart() throws Exception {

        String path = Path.of(
                getClass()
                        .getClassLoader()
                        .getResource("market-events-test.jsonl")
                        .toURI()
        ).toString();

        FileSource source = new FileSource(path);

        assertThrows(
                IllegalStateException.class,
                source::poll
        );
    }
}