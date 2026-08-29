package com.issaalsabeh.etl.connector.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KafkaSinkIntegrationTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "market-data-processed";

    @Test
    void shouldPublishEnrichedMarketEventToKafka() throws Exception {

        EnrichedMarketEvent event = new EnrichedMarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.2500"),
                100,
                Instant.now(),
                new BigDecimal("15025.0000")
        );

        KafkaSink sink = new KafkaSink(
                BOOTSTRAP_SERVERS,
                TOPIC
        );

        sink.start();
        sink.write(event);
        sink.stop();

        Properties properties = new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS
        );

        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "kafka-sink-test-" + UUID.randomUUID()
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

        ObjectMapper objectMapper =
                new ObjectMapper().findAndRegisterModules();

        EnrichedMarketEvent receivedEvent = null;
        String receivedKey = null;

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(properties)) {

            consumer.subscribe(List.of(TOPIC));

            long deadline =
                    System.currentTimeMillis() + 5000;

            while (System.currentTimeMillis() < deadline
                    && receivedEvent == null) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {

                    EnrichedMarketEvent candidate =
                            objectMapper.readValue(
                                    record.value(),
                                    EnrichedMarketEvent.class
                            );

                    if (candidate.eventId().equals(event.eventId())) {
                        receivedEvent = candidate;
                        receivedKey = record.key();
                        break;
                    }
                }
            }
        }

        assertThat(receivedEvent)
                .isNotNull();

        assertThat(receivedEvent)
                .isEqualTo(event);

        assertThat(receivedKey)
                .isEqualTo(event.symbol());
    }

    @Test
    void shouldThrowWhenWritingBeforeStart() {

        KafkaSink sink = new KafkaSink(
                BOOTSTRAP_SERVERS,
                TOPIC
        );

        EnrichedMarketEvent event = new EnrichedMarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.2500"),
                100,
                Instant.now(),
                new BigDecimal("15025.0000")
        );

        assertThatThrownBy(() -> sink.write(event))
                .isInstanceOf(IllegalStateException.class);
    }
}