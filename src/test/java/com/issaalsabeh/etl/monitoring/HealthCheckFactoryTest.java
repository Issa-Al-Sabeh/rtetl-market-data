
package com.issaalsabeh.etl.monitoring;

import com.issaalsabeh.etl.config.PipelineConfig;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HealthCheckFactoryTest {

    @Test
    void shouldCreateHealthChecksForProductionConfiguration() {

        PipelineConfig config = createBaseConfig();

        config.getPipeline().setSinks(
                List.of(
                        connector(
                                "kafka",
                                Map.of(
                                        "bootstrap.servers",
                                        "localhost:9092"
                                )
                        ),
                        connector(
                                "redis",
                                Map.of(
                                        "host", "127.0.0.1",
                                        "port", "6379"
                                )
                        ),
                        connector(
                                "postgres",
                                Map.of(
                                        "url",
                                        "jdbc:postgresql://localhost:5432/market_data",
                                        "username", "test_user",
                                        "password", "test_password"
                                )
                        )
                )
        );

        List<HealthCheck> healthChecks =
                HealthCheckFactory.create(config);

        assertThat(healthChecks)
                .hasSize(3);

        assertThat(healthChecks)
                .extracting(HealthCheck::name)
                .containsExactly(
                        "kafka",
                        "redis",
                        "postgres"
                );

        assertThat(healthChecks.get(0))
                .isInstanceOf(KafkaHealthCheck.class);

        assertThat(healthChecks.get(1))
                .isInstanceOf(RedisHealthCheck.class);

        assertThat(healthChecks.get(2))
                .isInstanceOf(PostgresHealthCheck.class);
    }


    @Test
    void shouldAssignCorrectCriticalityToHealthChecks() {

        PipelineConfig config = createBaseConfig();

        config.getPipeline().setSinks(
                List.of(
                        connector(
                                "postgres",
                                Map.of(
                                        "url",
                                        "jdbc:postgresql://localhost:5432/market_data",
                                        "username", "test_user",
                                        "password", "test_password"
                                )
                        ),
                        connector(
                                "redis",
                                Map.of(
                                        "host", "127.0.0.1",
                                        "port", "6379"
                                )
                        )
                )
        );

        List<HealthCheck> healthChecks =
                HealthCheckFactory.create(config);

        HealthCheck kafka = findHealthCheck(
                healthChecks,
                "kafka"
        );

        HealthCheck postgres = findHealthCheck(
                healthChecks,
                "postgres"
        );

        HealthCheck redis = findHealthCheck(
                healthChecks,
                "redis"
        );

        assertThat(kafka.isCritical())
                .isTrue();

        assertThat(postgres.isCritical())
                .isTrue();

        assertThat(redis.isCritical())
                .isFalse();
    }


    @Test
    void shouldAvoidDuplicateKafkaHealthChecks() {

        PipelineConfig config = createBaseConfig();

        // Kafka is already configured as the source
        // and as the dead-letter queue.

        // Add a Kafka sink using the same broker.

        config.getPipeline().setSinks(
                List.of(
                        connector(
                                "kafka",
                                Map.of(
                                        "bootstrap.servers",
                                        "localhost:9092"
                                )
                        )
                )
        );

        List<HealthCheck> healthChecks =
                HealthCheckFactory.create(config);

        assertThat(healthChecks)
                .hasSize(1);

        assertThat(healthChecks.get(0))
                .isInstanceOf(KafkaHealthCheck.class);

        assertThat(healthChecks.get(0).name())
                .isEqualTo("kafka");
    }


    @Test
    void shouldCreateKafkaHealthCheckForDeadLetterQueue() {

        PipelineConfig config = createBaseConfig();

        // Replace the Kafka source with a file source.

        config.getPipeline().setSource(
                connector(
                        "file",
                        Map.of(
                                "path",
                                "data/market-events.jsonl"
                        )
                )
        );

        // The console sink does not require
        // an external dependency health check.

        config.getPipeline().setSinks(
                List.of(
                        connector(
                                "console",
                                Map.of()
                        )
                )
        );

        // The dead-letter queue still uses Kafka.

        List<HealthCheck> healthChecks =
                HealthCheckFactory.create(config);

        assertThat(healthChecks)
                .hasSize(1);

        assertThat(healthChecks.get(0))
                .isInstanceOf(KafkaHealthCheck.class);

        assertThat(healthChecks.get(0).name())
                .isEqualTo("kafka");
    }


    @Test
    void shouldRejectMultipleDistinctKafkaConnections() {

        PipelineConfig config = createBaseConfig();

        // The source and DLQ use localhost:9092.
        // The sink uses a different Kafka cluster.

        config.getPipeline().setSinks(
                List.of(
                        connector(
                                "kafka",
                                Map.of(
                                        "bootstrap.servers",
                                        "localhost:9093"
                                )
                        )
                )
        );

        assertThatThrownBy(
                () -> HealthCheckFactory.create(config)
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Multiple distinct connections configured for kafka"
                );
    }


    @Test
    void shouldRejectMultipleDistinctRedisConnections() {

        PipelineConfig config = createBaseConfig();

        config.getPipeline().setSinks(
                List.of(
                        connector(
                                "redis",
                                Map.of(
                                        "host", "127.0.0.1",
                                        "port", "6379"
                                )
                        ),
                        connector(
                                "redis",
                                Map.of(
                                        "host", "127.0.0.1",
                                        "port", "6380"
                                )
                        )
                )
        );

        assertThatThrownBy(
                () -> HealthCheckFactory.create(config)
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Multiple distinct connections configured for redis"
                );
    }


    @Test
    void shouldCreateHealthChecksWithoutEstablishingConnections() {

        PipelineConfig config = createBaseConfig();

        // These connection settings point to services
        // that are not expected to be available.

        config.getPipeline().setSource(
                connector(
                        "kafka",
                        Map.of(
                                "bootstrap.servers",
                                "127.0.0.1:65534"
                        )
                )
        );

        config.getPipeline().setDeadLetterQueue(
                connector(
                        "kafka",
                        Map.of(
                                "bootstrap.servers",
                                "127.0.0.1:65534",
                                "topic",
                                "test-dlq"
                        )
                )
        );

        config.getPipeline().setSinks(
                List.of(
                        connector(
                                "postgres",
                                Map.of(
                                        "url",
                                        "jdbc:postgresql://127.0.0.1:65533/testdb",
                                        "username", "test",
                                        "password", "test"
                                )
                        ),
                        connector(
                                "redis",
                                Map.of(
                                        "host", "127.0.0.1",
                                        "port", "65532"
                                )
                        )
                )
        );

        // The factory must only construct health checks.
        // It must not call their check() methods.

        List<HealthCheck> healthChecks =
                HealthCheckFactory.create(config);

        assertThat(healthChecks)
                .hasSize(3);

        assertThat(healthChecks)
                .extracting(HealthCheck::name)
                .containsExactly(
                        "kafka",
                        "postgres",
                        "redis"
                );
    }


    @Test
    void shouldRejectNullPipelineConfiguration() {

        assertThatThrownBy(
                () -> HealthCheckFactory.create(null)
        )
                .isInstanceOf(IllegalArgumentException.class);
    }


    @Test
    void shouldRejectMissingRequiredConnectorProperty() {

        PipelineConfig config = createBaseConfig();

        config.getPipeline().setSinks(
                List.of(
                        connector(
                                "redis",
                                Map.of(
                                        "host", "127.0.0.1"
                                )
                        )
                )
        );

        // Redis is missing its required port property.

        assertThatThrownBy(
                () -> HealthCheckFactory.create(config)
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Missing required property: port"
                );
    }


    // =========================================================
    // Test Helpers
    // =========================================================

    private PipelineConfig createBaseConfig() {

        PipelineConfig config =
                new PipelineConfig();

        PipelineConfig.PipelineDefinition definition =
                new PipelineConfig.PipelineDefinition();

        definition.setName(
                "health-check-test-pipeline"
        );

        definition.setSource(
                connector(
                        "kafka",
                        Map.of(
                                "bootstrap.servers",
                                "localhost:9092",
                                "topic",
                                "market-data",
                                "group.id",
                                "market-data-etl"
                        )
                )
        );

        definition.setTransformations(
                List.of()
        );

        definition.setSinks(
                List.of(
                        connector(
                                "console",
                                Map.of()
                        )
                )
        );

        definition.setDeadLetterQueue(
                connector(
                        "kafka",
                        Map.of(
                                "bootstrap.servers",
                                "localhost:9092",
                                "topic",
                                "market-data-dlq"
                        )
                )
        );

        config.setPipeline(definition);

        return config;
    }


    private PipelineConfig.ConnectorConfig connector(
            String type,
            Map<String, String> properties
    ) {

        PipelineConfig.ConnectorConfig config =
                new PipelineConfig.ConnectorConfig();

        config.setType(type);
        config.setProperties(properties);

        return config;
    }


    private HealthCheck findHealthCheck(
            List<HealthCheck> healthChecks,
            String name
    ) {

        return healthChecks.stream()
                .filter(check -> check.name().equals(name))
                .findFirst()
                .orElseThrow(
                        () -> new AssertionError(
                                "Health check not found: " + name
                        )
                );
    }
}