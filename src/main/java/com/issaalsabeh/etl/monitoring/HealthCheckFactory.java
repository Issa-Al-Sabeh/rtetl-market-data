
package com.issaalsabeh.etl.monitoring;

import com.issaalsabeh.etl.config.PipelineConfig;
import com.issaalsabeh.etl.config.PipelineConfigValidator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class HealthCheckFactory {

    private HealthCheckFactory() {
    }


    // =========================================================
    // Create Health Checks
    // =========================================================

    public static List<HealthCheck> create(
            PipelineConfig config
    ) {

        PipelineConfigValidator.validate(config);

        PipelineConfig.PipelineDefinition definition =
                config.getPipeline();

        Map<String, HealthCheck> healthChecks =
                new LinkedHashMap<>();

        Map<String, ConnectionKey> registeredConnections =
                new LinkedHashMap<>();


        // -----------------------------------------------------
        // Register Source Health Check
        // -----------------------------------------------------

        PipelineConfig.ConnectorConfig sourceConfig =
                definition.getSource();

        if ("kafka".equalsIgnoreCase(sourceConfig.getType())) {

            registerHealthCheck(
                    sourceConfig,
                    healthChecks,
                    registeredConnections
            );
        }


        // -----------------------------------------------------
        // Register Sink Health Checks
        // -----------------------------------------------------

        for (PipelineConfig.ConnectorConfig sinkConfig
                : definition.getSinks()) {

            registerHealthCheck(
                    sinkConfig,
                    healthChecks,
                    registeredConnections
            );
        }


        // -----------------------------------------------------
        // Register Dead Letter Queue Health Check
        // -----------------------------------------------------

        PipelineConfig.ConnectorConfig dlqConfig =
                definition.getDeadLetterQueue();

        if ("kafka".equalsIgnoreCase(dlqConfig.getType())) {

            registerHealthCheck(
                    dlqConfig,
                    healthChecks,
                    registeredConnections
            );
        }


        // -----------------------------------------------------
        // Return Immutable Health Check List
        // -----------------------------------------------------

        return List.copyOf(healthChecks.values());
    }


    // =========================================================
    // Register Individual Health Check
    // =========================================================

    private static void registerHealthCheck(
            PipelineConfig.ConnectorConfig connectorConfig,
            Map<String, HealthCheck> healthChecks,
            Map<String, ConnectionKey> registeredConnections
    ) {

        String type =
                connectorConfig.getType().toLowerCase(
                        java.util.Locale.ROOT
                );


        switch (type) {

            // -------------------------------------------------
            // Kafka
            // -------------------------------------------------

            case "kafka": {

                String bootstrapServers =
                        getRequiredProperty(
                                connectorConfig,
                                "bootstrap.servers"
                        );

                KafkaHealthCheck healthCheck =
                        new KafkaHealthCheck(
                                bootstrapServers
                        );

                ConnectionKey connectionKey =
                        new ConnectionKey(
                                "kafka",
                                bootstrapServers,
                                null,
                                null
                        );

                register(
                        healthCheck,
                        connectionKey,
                        healthChecks,
                        registeredConnections
                );

                break;
            }


            // -------------------------------------------------
            // PostgreSQL
            // -------------------------------------------------

            case "postgres": {

                String url =
                        getRequiredProperty(
                                connectorConfig,
                                "url"
                        );

                String username =
                        getRequiredProperty(
                                connectorConfig,
                                "username"
                        );

                String password =
                        getRequiredProperty(
                                connectorConfig,
                                "password"
                        );

                PostgresHealthCheck healthCheck =
                        new PostgresHealthCheck(
                                url,
                                username,
                                password
                        );

                ConnectionKey connectionKey =
                        new ConnectionKey(
                                "postgres",
                                url,
                                username,
                                password
                        );

                register(
                        healthCheck,
                        connectionKey,
                        healthChecks,
                        registeredConnections
                );

                break;
            }


            // -------------------------------------------------
            // Redis
            // -------------------------------------------------

            case "redis": {

                String host =
                        getRequiredProperty(
                                connectorConfig,
                                "host"
                        );

                String portValue =
                        getRequiredProperty(
                                connectorConfig,
                                "port"
                        );

                int port = Integer.parseInt(portValue);

                RedisHealthCheck healthCheck =
                        new RedisHealthCheck(
                                host,
                                port
                        );

                ConnectionKey connectionKey =
                        new ConnectionKey(
                                "redis",
                                host + ":" + port,
                                null,
                                null
                        );

                register(
                        healthCheck,
                        connectionKey,
                        healthChecks,
                        registeredConnections
                );

                break;
            }


            // -------------------------------------------------
            // Connectors Without External Dependency Checks
            // -------------------------------------------------

            default:
                break;
        }
    }


    // =========================================================
    // Register Health Check Without Duplicates
    // =========================================================

    private static void register(
            HealthCheck healthCheck,
            ConnectionKey connectionKey,
            Map<String, HealthCheck> healthChecks,
            Map<String, ConnectionKey> registeredConnections
    ) {

        String name = healthCheck.name();

        ConnectionKey existingConnection =
                registeredConnections.get(name);


        // No health check registered for this component yet.

        if (existingConnection == null) {

            healthChecks.put(
                    name,
                    healthCheck
            );

            registeredConnections.put(
                    name,
                    connectionKey
            );

            return;
        }


        // The same dependency has already been registered.

        if (existingConnection.equals(connectionKey)) {

            return;
        }


        // Different connections would produce the same
        // component name and overwrite each other's results.

        throw new IllegalArgumentException(
                "Multiple distinct connections configured for "
                        + name
                        + ". Unique health-check names are required."
        );
    }


    // =========================================================
    // Read Required Connector Property
    // =========================================================

    private static String getRequiredProperty(
            PipelineConfig.ConnectorConfig connectorConfig,
            String propertyName
    ) {

        Map<String, String> properties =
                Objects.requireNonNull(
                        connectorConfig.getProperties(),
                        "Connector properties must not be null"
                );

        String value = properties.get(propertyName);

        if (value == null || value.isBlank()) {

            throw new IllegalArgumentException(
                    "Missing required property: "
                            + propertyName
                            + " for connector type: "
                            + connectorConfig.getType()
            );
        }

        return value;
    }


    // =========================================================
    // Connection Identity
    // =========================================================

    private record ConnectionKey(
            String type,
            String endpoint,
            String username,
            String password
    ) {
    }
}