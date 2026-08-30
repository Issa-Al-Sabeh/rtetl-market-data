package com.issaalsabeh.etl.connector.postgres;

import com.issaalsabeh.etl.core.Sink;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import com.issaalsabeh.etl.model.MarketEvent;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;

public class PostgresSink implements Sink<EnrichedMarketEvent> {
    private HikariDataSource dataSource;
    private final String url;
    private final String username;
    private final String password;

    private static final String INSERT_SQL = """
        INSERT INTO market_events (
            event_id,
            symbol,
            price,
            volume,
            timestamp,
            notional_value
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (event_id) DO NOTHING
        """;

    public PostgresSink(String url, String username, String password){
        this.url = url;
        this.username = username;
        this.password = password;
    }
    @Override
    public void start() {
        try {
            HikariConfig config = new HikariConfig();

            config.setJdbcUrl(url);
            config.setUsername(username);
            config.setPassword(password);

            config.setMaximumPoolSize(10);
            config.setMinimumIdle(2);

            dataSource = new HikariDataSource(config);

        } catch (RuntimeException e) {
            throw new RuntimeException(
                    "Failed to initialize PostgreSQL connection pool",
                    e
            );
        }
    }

    @Override
    public void write(EnrichedMarketEvent event) {
        if (dataSource == null) {
            throw new IllegalStateException("PostgresSink has not been started");
        }

        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(INSERT_SQL)
        ) {

            statement.setObject(1, event.eventId());
            statement.setString(2, event.symbol());
            statement.setBigDecimal(3, event.price());
            statement.setLong(4, event.volume());
            statement.setTimestamp(5, Timestamp.from(event.timestamp()));
            statement.setBigDecimal(6, event.notionalValue());

            statement.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to insert market event: " + event.eventId(),
                    e
            );
        }
    }

    @Override
    public void stop() {
        if (dataSource != null) {
            try {
                dataSource.close();
            } catch (RuntimeException e) {
                throw new RuntimeException(
                        "Failed to close PostgreSQL connection pool",
                        e
                );
            } finally {
                dataSource = null;
            }
        }
    }

    @Override
    public Class<?> getInputType() {
        return EnrichedMarketEvent.class;
    }
}
