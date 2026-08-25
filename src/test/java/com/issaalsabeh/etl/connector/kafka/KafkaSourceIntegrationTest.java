package com.issaalsabeh.etl.connector.kafka;

import com.issaalsabeh.etl.model.MarketEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;

class KafkaSourceIntegrationTest {

    private KafkaSource kafkaSource;
    private KafkaProducer<String, String> producer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {

        kafkaSource = new KafkaSource();

        Properties properties = new Properties();

        properties.put(
                "bootstrap.servers",
                "localhost:9092"
        );

        properties.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        properties.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        producer = new KafkaProducer<>(properties);

        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();

        kafkaSource.start();
    }

    @AfterEach
    void tearDown() {
        kafkaSource.stop();
        producer.close();
    }

    @Test
    void shouldConsumeMarketEventFromKafka() throws Exception {

        MarketEvent expectedEvent = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("250.1234"),
                5000,
                Instant.now()
        );

        String json =
                objectMapper.writeValueAsString(expectedEvent);

        ProducerRecord<String, String> record =
                new ProducerRecord<>(
                        "market-data",
                        expectedEvent.symbol(),
                        json
                );

        producer.send(record).get();

        MarketEvent receivedEvent = waitForEvent(expectedEvent.eventId());

        assertNotNull(receivedEvent);

        assertEquals(
                expectedEvent.eventId(),
                receivedEvent.eventId()
        );

        assertEquals(
                expectedEvent.symbol(),
                receivedEvent.symbol()
        );

        assertEquals(
                expectedEvent.price(),
                receivedEvent.price()
        );

        assertEquals(
                expectedEvent.volume(),
                receivedEvent.volume()
        );
    }

    @Test
    void shouldIgnoreMalformedMessageAndContinueConsuming() throws Exception {

        ProducerRecord<String, String> malformedRecord =
                new ProducerRecord<>(
                        "market-data",
                        "AAPL",
                        "this-is-not-valid-json"
                );

        producer.send(malformedRecord).get();

        MarketEvent validEvent = new MarketEvent(
                UUID.randomUUID(),
                "MSFT",
                new BigDecimal("300.5000"),
                10000,
                Instant.now()
        );

        String validJson =
                objectMapper.writeValueAsString(validEvent);

        ProducerRecord<String, String> validRecord =
                new ProducerRecord<>(
                        "market-data",
                        validEvent.symbol(),
                        validJson
                );

        producer.send(validRecord).get();

        MarketEvent receivedEvent =
                waitForEvent(validEvent.eventId());

        assertNotNull(receivedEvent);

        assertEquals(
                validEvent.eventId(),
                receivedEvent.eventId()
        );

        assertEquals(
                "MSFT",
                receivedEvent.symbol()
        );
    }

    private MarketEvent waitForEvent(UUID expectedEventId) {

        long timeout = System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < timeout) {

            MarketEvent event = kafkaSource.poll();

            if (event != null &&
                    event.eventId().equals(expectedEventId)) {

                return event;
            }
        }

        return null;
    }
}