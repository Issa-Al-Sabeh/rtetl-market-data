package com.issaalsabeh.etl.connector.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.issaalsabeh.etl.core.dlq.DeadLetterRecord;
import com.issaalsabeh.etl.model.MarketEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaDeadLetterQueueIntegrationTest {

    private static final String BOOTSTRAP_SERVERS =
            "localhost:9092";

    private static final String TOPIC =
            "market-data-dlq";

    private KafkaDeadLetterQueue deadLetterQueue;
    private KafkaConsumer<String, String> consumer;

    private final ObjectMapper objectMapper =
            new ObjectMapper().findAndRegisterModules();

    @AfterEach
    void tearDown() {

        if (deadLetterQueue != null) {
            deadLetterQueue.stop();
        }

        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void shouldPublishDeadLetterRecordToKafka()
            throws Exception {

        MarketEvent event =
                new MarketEvent(
                        UUID.randomUUID(),
                        "AAPL",
                        new BigDecimal("150.2500"),
                        1000L,
                        Instant.now()
                );

        Instant failureTimestamp =
                Instant.now();

        DeadLetterRecord deadLetterRecord =
                new DeadLetterRecord(
                        event,
                        "PostgresSink",
                        "java.sql.SQLException",
                        "Database unavailable",
                        failureTimestamp,
                        2
                );

        deadLetterQueue =
                new KafkaDeadLetterQueue(
                        BOOTSTRAP_SERVERS,
                        TOPIC
                );

        deadLetterQueue.start();

        deadLetterQueue.publish(
                deadLetterRecord
        );

        consumer = createConsumer();

        consumer.subscribe(
                List.of(TOPIC)
        );

        JsonNode receivedRecord =
                waitForRecord(event.eventId());

        assertThat(receivedRecord)
                .isNotNull();

        assertThat(
                receivedRecord
                        .get("failedSink")
                        .asText()
        )
                .isEqualTo("PostgresSink");

        assertThat(
                receivedRecord
                        .get("errorType")
                        .asText()
        )
                .isEqualTo(
                        "java.sql.SQLException"
                );

        assertThat(
                receivedRecord
                        .get("errorMessage")
                        .asText()
        )
                .isEqualTo(
                        "Database unavailable"
                );

        assertThat(
                receivedRecord
                        .get("retryCount")
                        .asInt()
        )
                .isEqualTo(2);

        assertThat(
                receivedRecord
                        .get("failureTimestamp")
                        .asText()
        )
                .isEqualTo(
                        failureTimestamp.toString()
                );

        JsonNode originalEvent =
                receivedRecord.get(
                        "originalEvent"
                );

        assertThat(originalEvent)
                .isNotNull();

        assertThat(
                originalEvent
                        .get("eventId")
                        .asText()
        )
                .isEqualTo(
                        event.eventId().toString()
                );

        assertThat(
                originalEvent
                        .get("symbol")
                        .asText()
        )
                .isEqualTo("AAPL");

        assertThat(
                originalEvent
                        .get("volume")
                        .asLong()
        )
                .isEqualTo(1000L);
    }

    @Test
    void shouldThrowWhenPublishingBeforeStart() {

        deadLetterQueue =
                new KafkaDeadLetterQueue(
                        BOOTSTRAP_SERVERS,
                        TOPIC
                );

        DeadLetterRecord record =
                new DeadLetterRecord(
                        "test-event",
                        "TestSink",
                        "java.lang.RuntimeException",
                        "Test failure",
                        Instant.now(),
                        2
                );

        assertThatThrownBy(
                () -> deadLetterQueue.publish(record)
        )
                .isInstanceOf(
                        IllegalStateException.class
                )
                .hasMessage(
                        "Dead-letter queue has not been started"
                );
    }

    private KafkaConsumer<String, String>
    createConsumer() {

        Properties properties =
                new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS
        );

        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "dlq-integration-test-"
                        + UUID.randomUUID()
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        properties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false"
        );

        return new KafkaConsumer<>(
                properties
        );
    }

    private JsonNode waitForRecord(
            UUID eventId
    ) throws Exception {

        long deadline =
                System.currentTimeMillis()
                        + 10_000;

        while (System.currentTimeMillis()
                < deadline) {

            ConsumerRecords<String, String> records =
                    consumer.poll(
                            Duration.ofMillis(500)
                    );

            for (ConsumerRecord<String, String> record
                    : records) {

                JsonNode json =
                        objectMapper.readTree(
                                record.value()
                        );

                JsonNode originalEvent =
                        json.get(
                                "originalEvent"
                        );

                if (originalEvent == null) {
                    continue;
                }

                JsonNode receivedEventId =
                        originalEvent.get(
                                "eventId"
                        );

                if (receivedEventId != null
                        && eventId.toString()
                        .equals(
                                receivedEventId.asText()
                        )) {

                    return json;
                }
            }
        }

        return null;
    }
}