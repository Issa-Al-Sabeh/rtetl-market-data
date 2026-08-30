package com.issaalsabeh.etl.core.factory;

import com.issaalsabeh.etl.config.DeadLetterQueueConfig;
import com.issaalsabeh.etl.connector.kafka.KafkaDeadLetterQueue;
import com.issaalsabeh.etl.core.dlq.DeadLetterQueue;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeadLetterQueueFactoryTest {

    @Test
    void shouldCreateKafkaDeadLetterQueue() {

        DeadLetterQueueConfig config =
                new DeadLetterQueueConfig(
                        "kafka",
                        Map.of(
                                "bootstrap.servers",
                                "localhost:9092",
                                "topic",
                                "market-data-dlq"
                        )
                );

        DeadLetterQueue deadLetterQueue =
                DeadLetterQueueFactory.create(config);

        assertThat(deadLetterQueue)
                .isInstanceOf(KafkaDeadLetterQueue.class);
    }

    @Test
    void shouldCreateKafkaDeadLetterQueueCaseInsensitive() {

        DeadLetterQueueConfig config =
                new DeadLetterQueueConfig(
                        "KAFKA",
                        Map.of(
                                "bootstrap.servers",
                                "localhost:9092",
                                "topic",
                                "market-data-dlq"
                        )
                );

        DeadLetterQueue deadLetterQueue =
                DeadLetterQueueFactory.create(config);

        assertThat(deadLetterQueue)
                .isInstanceOf(KafkaDeadLetterQueue.class);
    }

    @Test
    void shouldRejectUnknownDlqType() {

        DeadLetterQueueConfig config =
                new DeadLetterQueueConfig(
                        "unknown",
                        Map.of()
                );

        assertThatThrownBy(
                () -> DeadLetterQueueFactory.create(config)
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Unsupported dead letter queue type"
                );
    }

    @Test
    void shouldRejectNullConfig() {

        assertThatThrownBy(
                () -> DeadLetterQueueFactory.create(null)
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Dead letter queue config cannot be null"
                );
    }

    @Test
    void shouldRejectMissingTopic() {

        DeadLetterQueueConfig config =
                new DeadLetterQueueConfig(
                        "kafka",
                        Map.of(
                                "bootstrap.servers",
                                "localhost:9092"
                        )
                );

        assertThatThrownBy(
                () -> DeadLetterQueueFactory.create(config)
        )
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectMissingBootstrapServers() {

        DeadLetterQueueConfig config =
                new DeadLetterQueueConfig(
                        "kafka",
                        Map.of(
                                "topic",
                                "market-data-dlq"
                        )
                );

        assertThatThrownBy(
                () -> DeadLetterQueueFactory.create(config)
        )
                .isInstanceOf(IllegalArgumentException.class);
    }
}