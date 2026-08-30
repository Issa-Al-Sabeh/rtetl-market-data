package com.issaalsabeh.etl.connector.kafka;

import com.issaalsabeh.etl.model.MarketEvent;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;

class KafkaSourceIntegrationTest {

    private static final String BOOTSTRAP_SERVERS =
            "localhost:9092";
    private KafkaSource kafkaSource;
    private KafkaProducer<String, String> producer;
    private ObjectMapper objectMapper;

    private AdminClient adminClient;
    private String testTopic;

    @BeforeEach
    void setUp() throws Exception{

        testTopic =
                "market-data-test-" + UUID.randomUUID();

        Properties adminProperties = new Properties();

        adminProperties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS
        );

        adminClient =
                AdminClient.create(adminProperties);

        adminClient.createTopics(
                List.of(
                        new NewTopic(
                                testTopic,
                                1,
                                (short) 1
                        )
                )
        ).all().get();

        kafkaSource = new KafkaSource(
                BOOTSTRAP_SERVERS,
                testTopic,
                "kafka-source-test-" + UUID.randomUUID(),
                "earliest"
        );

        Properties properties = new Properties();

        properties.put(
                "bootstrap.servers",
                BOOTSTRAP_SERVERS
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
    void tearDown() throws Exception {

        kafkaSource.stop();
        producer.close();

        adminClient.deleteTopics(
                List.of(testTopic)
        ).all().get();

        adminClient.close();
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
                        testTopic,
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
                        testTopic,
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
                        testTopic,
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