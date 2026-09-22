
package com.issaalsabeh.etl.monitoring;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.junit.jupiter.api.Assertions.assertTimeout;

class RedisHealthCheckIntegrationTest {


    @Test
    void shouldReturnHealthyWhenRedisIsAvailable() {

        RedisHealthCheck healthCheck =
                new RedisHealthCheck(
                        "127.0.0.1",
                        6379
                );

        assertThat(healthCheck.name())
                .isEqualTo("redis");

        assertThat(healthCheck.isCritical())
                .isFalse();

        assertThatCode(healthCheck::check)
                .doesNotThrowAnyException();
    }


    @Test
    void shouldFailWhenRedisIsUnavailable()
            throws Exception {

        // Reserve a local port that is not serving Redis.

        try (ServerSocket socket = new ServerSocket(
                0,
                1,
                InetAddress.getByName("127.0.0.1")
        )) {

            int unavailablePort =
                    socket.getLocalPort();

            RedisHealthCheck healthCheck =
                    new RedisHealthCheck(
                            "127.0.0.1",
                            unavailablePort
                    );

            assertTimeout(
                    Duration.ofSeconds(10),
                    () -> assertThatThrownBy(
                            healthCheck::check
                    ).isInstanceOf(Exception.class)
            );
        }
    }


    @Test
    void shouldRejectInvalidRedisConfiguration() {

        assertThatThrownBy(
                () -> new RedisHealthCheck(
                        "",
                        6379
                )
        )
                .isInstanceOf(IllegalArgumentException.class);


        assertThatThrownBy(
                () -> new RedisHealthCheck(
                        "127.0.0.1",
                        -1
                )
        )
                .isInstanceOf(IllegalArgumentException.class);


        assertThatThrownBy(
                () -> new RedisHealthCheck(
                        "127.0.0.1",
                        65536
                )
        )
                .isInstanceOf(IllegalArgumentException.class);
    }
}