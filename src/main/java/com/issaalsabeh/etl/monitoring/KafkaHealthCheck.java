
package com.issaalsabeh.etl.monitoring;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigException;

import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaHealthCheck implements HealthCheck {

    private static final int TIMEOUT_MS = 2000;

    private final String bootstrapServers;

    public KafkaHealthCheck(String bootstrapServers) {

        if (bootstrapServers == null
                || bootstrapServers.isBlank()) {

            throw new IllegalArgumentException(
                    "Kafka bootstrap servers must not be null or blank"
            );
        }

        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public String name() {
        return "kafka";
    }

    @Override
    public boolean isCritical() {
        return true;
    }

    @Override
    public void check() throws Exception {

        Properties properties = new Properties();

        properties.put(
                "bootstrap.servers",
                bootstrapServers
        );

        properties.put(
                "request.timeout.ms",
                TIMEOUT_MS
        );

        properties.put(
                "default.api.timeout.ms",
                TIMEOUT_MS
        );

        try (AdminClient adminClient =
                     AdminClient.create(properties)) {

            DescribeClusterResult result =
                    adminClient.describeCluster();

            Collection<Node> nodes =
                    result.nodes()
                            .get(
                                    TIMEOUT_MS,
                                    TimeUnit.MILLISECONDS
                            );

            if (nodes == null || nodes.isEmpty()) {

                throw new IllegalStateException(
                        "Kafka cluster has no available brokers"
                );
            }
        }
    }
}