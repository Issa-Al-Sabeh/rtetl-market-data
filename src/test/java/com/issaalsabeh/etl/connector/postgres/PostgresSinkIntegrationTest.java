package com.issaalsabeh.etl.connector.postgres;

import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class PostgresSinkIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16")
                    .withDatabaseName("market_data_test")
                    .withUsername("test_user")
                    .withPassword("test_password");

    @BeforeEach
    void setUp() throws Exception {
        try (
                Connection connection = DriverManager.getConnection(
                        POSTGRES.getJdbcUrl(),
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword()
                );
                Statement statement = connection.createStatement()
        ) {

            statement.execute("""
                    CREATE TABLE IF NOT EXISTS market_events (
                        event_id UUID PRIMARY KEY,
                        symbol VARCHAR(20) NOT NULL,
                        price NUMERIC(19,4) NOT NULL,
                        volume BIGINT NOT NULL,
                        timestamp TIMESTAMPTZ NOT NULL,
                        notional_value NUMERIC NOT NULL,
                        processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """);

            statement.execute("DELETE FROM market_events");
        }
    }

    @Test
    void shouldInsertEnrichedMarketEventIntoPostgres() throws Exception {

        PostgresSink sink = createSink();

        EnrichedMarketEvent event = new EnrichedMarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("182.4500"),
                5000L,
                Instant.parse("2026-08-25T18:30:45.123456Z"),
                new BigDecimal("912250.0000")
        );

        try {
            sink.start();
            sink.write(event);

            try (
                    Connection connection = DriverManager.getConnection(
                            POSTGRES.getJdbcUrl(),
                            POSTGRES.getUsername(),
                            POSTGRES.getPassword()
                    );
                    PreparedStatement statement = connection.prepareStatement(
                            """
                            SELECT event_id,
                                   symbol,
                                   price,
                                   volume,
                                   timestamp,
                                   notional_value,
                                   processed_at
                            FROM market_events
                            WHERE event_id = ?
                            """
                    )
            ) {

                statement.setObject(1, event.eventId());

                try (ResultSet resultSet = statement.executeQuery()) {

                    assertThat(resultSet.next()).isTrue();

                    assertThat(resultSet.getObject("event_id", UUID.class))
                            .isEqualTo(event.eventId());

                    assertThat(resultSet.getString("symbol"))
                            .isEqualTo(event.symbol());

                    assertThat(resultSet.getBigDecimal("price"))
                            .isEqualByComparingTo(event.price());

                    assertThat(resultSet.getLong("volume"))
                            .isEqualTo(event.volume());

                    assertThat(resultSet.getTimestamp("timestamp").toInstant())
                            .isEqualTo(event.timestamp());

                    assertThat(resultSet.getBigDecimal("notional_value"))
                            .isEqualByComparingTo(event.notionalValue());

                    assertThat(resultSet.getTimestamp("processed_at"))
                            .isNotNull();

                    assertThat(resultSet.next()).isFalse();
                }
            }

        } finally {
            sink.stop();
        }
    }

    @Test
    void shouldThrowExceptionWhenWritingBeforeStart() {

        PostgresSink sink = createSink();

        EnrichedMarketEvent event = createEvent();

        assertThatThrownBy(() -> sink.write(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PostgresSink has not been started");
    }

    @Test
    void shouldStoreOnlyOneRecordWhenSameEventIsWrittenTwice() throws Exception {

        PostgresSink sink = createSink();

        EnrichedMarketEvent event = new EnrichedMarketEvent(
                UUID.randomUUID(),
                "MSFT",
                new BigDecimal("250.0000"),
                1000L,
                Instant.parse("2026-08-25T18:30:45Z"),
                new BigDecimal("250000.0000")
        );

        try {
            sink.start();

            sink.write(event);
            sink.write(event);

            try (
                    Connection connection = DriverManager.getConnection(
                            POSTGRES.getJdbcUrl(),
                            POSTGRES.getUsername(),
                            POSTGRES.getPassword()
                    );
                    PreparedStatement statement = connection.prepareStatement(
                            """
                            SELECT COUNT(*)
                            FROM market_events
                            WHERE event_id = ?
                            """
                    )
            ) {

                statement.setObject(1, event.eventId());

                try (ResultSet resultSet = statement.executeQuery()) {

                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getInt(1)).isEqualTo(1);
                }
            }

        } finally {
            sink.stop();
        }
    }

    @Test
    void shouldFailToStartWithInvalidCredentials() {

        PostgresSink sink = new PostgresSink(
                POSTGRES.getJdbcUrl(),
                "wrong_user",
                "wrong_password"
        );

        assertThatThrownBy(sink::start)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Failed to initialize PostgreSQL connection pool"
                );
    }

    private PostgresSink createSink() {
        return new PostgresSink(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword()
        );
    }

    private EnrichedMarketEvent createEvent() {
        return new EnrichedMarketEvent(
                UUID.randomUUID(),
                "TSLA",
                new BigDecimal("300.0000"),
                2000L,
                Instant.parse("2026-08-25T18:30:45Z"),
                new BigDecimal("600000.0000")
        );
    }
}