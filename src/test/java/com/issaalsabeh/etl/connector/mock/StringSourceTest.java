package com.issaalsabeh.etl.connector.mock;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StringSourceTest {

    @Test
    void shouldProduceStringAfterStart() {

        StringSource source = new StringSource();

        source.start();

        String value = source.poll();

        assertThat(value)
                .isEqualTo("hello");

        source.stop();
    }

    @Test
    void shouldReportStringOutputType() {

        StringSource source = new StringSource();

        assertThat(source.getOutputType())
                .isEqualTo(String.class);
    }

    @Test
    void shouldRejectPollBeforeStart() {

        StringSource source = new StringSource();

        assertThatThrownBy(source::poll)
                .isInstanceOf(IllegalStateException.class);
    }
}