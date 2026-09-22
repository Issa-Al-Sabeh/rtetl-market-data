
package com.issaalsabeh.etl.monitoring;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.junit.jupiter.api.Assertions.assertTimeout;

@Testcontainers
class PostgresHealthCheckIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16");


    @Test
    void shouldReturnHealthyWhenPostgresIsAvailable() {

        PostgresHealthCheck healthCheck =
                new PostgresHealthCheck(
                        postgres.getJdbcUrl(),
                        postgres.getUsername(),
                        postgres.getPassword()
                );

        assertThat(healthCheck.name())
                .isEqualTo("postgres");

        assertThat(healthCheck.isCritical())
                .isTrue();

        assertThatCode(healthCheck::check)
                .doesNotThrowAnyException();
    }


    @Test
    void shouldFailWhenPostgresCredentialsAreInvalid() {

        PostgresHealthCheck healthCheck =
                new PostgresHealthCheck(
                        postgres.getJdbcUrl(),
                        "incorrect_user",
                        "incorrect_password"
                );

        assertTimeout(
                Duration.ofSeconds(10),
                () -> assertThatThrownBy(
                        healthCheck::check
                ).isInstanceOf(Exception.class)
        );
    }


    @Test
    void shouldFailWhenPostgresIsUnavailable()
            throws Exception {

        // Reserve a local port that is not serving PostgreSQL.

        try (ServerSocket socket = new ServerSocket(
                0,
                1,
                InetAddress.getByName("127.0.0.1")
        )) {

            int unavailablePort =
                    socket.getLocalPort();

            String unavailableUrl =
                    "jdbc:postgresql://127.0.0.1:"
                            + unavailablePort
                            + "/testdb";

            PostgresHealthCheck healthCheck =
                    new PostgresHealthCheck(
                            unavailableUrl,
                            "test",
                            "test"
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
    void shouldNotRequireMarketEventsTable() {

        // The Testcontainer has a fresh database.
        // We do not create the market_events table.

        PostgresHealthCheck healthCheck =
                new PostgresHealthCheck(
                        postgres.getJdbcUrl(),
                        postgres.getUsername(),
                        postgres.getPassword()
                );

        assertThatCode(healthCheck::check)
                .doesNotThrowAnyException();
    }
}