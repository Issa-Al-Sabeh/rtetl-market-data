
package com.issaalsabeh.etl.monitoring;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaHealthCheckIntegrationTest {

    @Test
    void shouldReturnHealthyWhenKafkaIsAvailable() {

        KafkaHealthCheck healthCheck =
                new KafkaHealthCheck(
                        "localhost:9092"
                );

        assertThat(healthCheck.name())
                .isEqualTo("kafka");

        assertThat(healthCheck.isCritical())
                .isTrue();

        assertThatCode(healthCheck::check)
                .doesNotThrowAnyException();
    }


    @Test
    void shouldFailWhenKafkaIsUnavailable()
            throws IOException {

        int unavailablePort;

        // Find an available local port.
        // Once the socket closes, no server will
        // be listening on this port during the test.

        try (ServerSocket socket =
                     new ServerSocket(0)) {

            unavailablePort =
                    socket.getLocalPort();
        }

        KafkaHealthCheck healthCheck =
                new KafkaHealthCheck(
                        "127.0.0.1:" + unavailablePort
                );

        assertThatThrownBy(healthCheck::check)
                .isInstanceOf(Exception.class);
    }
}