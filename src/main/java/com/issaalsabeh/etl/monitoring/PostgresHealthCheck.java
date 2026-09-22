
package com.issaalsabeh.etl.monitoring;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class PostgresHealthCheck implements HealthCheck {

    private static final int TIMEOUT_SECONDS = 2;

    private static final String HEALTH_QUERY = "SELECT 1";

    private final String url;
    private final String username;
    private final String password;


    public PostgresHealthCheck(
            String url,
            String username,
            String password
    ) {

        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException(
                    "PostgreSQL URL must not be null or blank"
            );
        }

        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException(
                    "PostgreSQL username must not be null or blank"
            );
        }

        if (password == null) {
            throw new IllegalArgumentException(
                    "PostgreSQL password must not be null"
            );
        }

        this.url = url;
        this.username = username;
        this.password = password;
    }


    @Override
    public String name() {
        return "postgres";
    }


    @Override
    public boolean isCritical() {
        return true;
    }


    @Override
    public void check() throws Exception {

        Properties properties = new Properties();

        properties.setProperty(
                "user",
                username
        );

        properties.setProperty(
                "password",
                password
        );

        properties.setProperty(
                "connectTimeout",
                String.valueOf(TIMEOUT_SECONDS)
        );

        properties.setProperty(
                "socketTimeout",
                String.valueOf(TIMEOUT_SECONDS)
        );


        try (Connection connection =
                     DriverManager.getConnection(
                             url,
                             properties
                     )) {

            connection.setReadOnly(true);

            try (PreparedStatement statement =
                         connection.prepareStatement(
                                 HEALTH_QUERY
                         )) {

                statement.setQueryTimeout(TIMEOUT_SECONDS);

                try (ResultSet resultSet =
                             statement.executeQuery()) {

                    if (!resultSet.next()
                            || resultSet.getInt(1) != 1) {

                        throw new IllegalStateException(
                                "PostgreSQL health check returned an unexpected result"
                        );
                    }
                }
            }
        }
    }
}