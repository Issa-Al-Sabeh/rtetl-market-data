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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
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

    @Test
    void shouldPreserveEventIdAcrossDuplicateKafkaDelivery() throws Exception {

        UUID eventId = UUID.randomUUID();

        MarketEvent event = new MarketEvent(
                eventId,
                "AAPL",
                new BigDecimal("150.2500"),
                1000L,
                Instant.parse("2026-08-31T00:00:00Z")
        );

        String json = objectMapper.writeValueAsString(event);

        producer.send(
                new ProducerRecord<>(
                        testTopic,
                        event.symbol(),
                        json
                )
        ).get();

        producer.send(
                new ProducerRecord<>(
                        testTopic,
                        event.symbol(),
                        json
                )
        ).get();

        producer.flush();

        KafkaSource source = new KafkaSource(
                "localhost:9092",
                testTopic,
                "duplicate-test-" + UUID.randomUUID(),
                "earliest"
        );

        try {
            source.start();

            MarketEvent first = waitForEventWithId(source, eventId);
            MarketEvent second = waitForEventWithId(source, eventId);

            assertThat(first).isNotNull();
            assertThat(second).isNotNull();

            assertThat(first.eventId())
                    .isEqualTo(eventId);

            assertThat(second.eventId())
                    .isEqualTo(eventId);

            assertThat(first)
                    .isEqualTo(second);

        } finally {
            source.stop();
        }
    }

    @Test
    void shouldRedeliverEventWhenOffsetWasNotCommitted() throws Exception {

        String topic =
                "market-data-offset-test-" + UUID.randomUUID();

        String groupId =
                "offset-test-group-" + UUID.randomUUID();

        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.2500"),
                1000,
                Instant.now()
        );

        String json = objectMapper.writeValueAsString(event);

        producer.send(
                new ProducerRecord<>(
                        topic,
                        event.symbol(),
                        json
                )
        ).get();

        KafkaSource firstSource =
                new KafkaSource(
                        "localhost:9092",
                        topic,
                        groupId,
                        "earliest"
                );

        firstSource.start();

        MarketEvent firstDelivery =
                waitForEventWithId(
                        firstSource,
                        event.eventId()
                );

        assertThat(firstDelivery)
                .isEqualTo(event);

        // IMPORTANT:
        // do NOT call firstSource.commit()

        firstSource.stop();

        KafkaSource restartedSource =
                new KafkaSource(
                        "localhost:9092",
                        topic,
                        groupId,
                        "earliest"
                );

        try {
            restartedSource.start();

            MarketEvent secondDelivery =
                    waitForEventWithId(
                            restartedSource,
                            event.eventId()
                    );

            assertThat(secondDelivery)
                    .isEqualTo(event);

        } finally {
            restartedSource.stop();
        }
    }

    @Test
    void shouldNotRedeliverEventAfterOffsetIsCommitted() throws Exception {

        String topic =
                "market-data-offset-test-" + UUID.randomUUID();

        String groupId =
                "offset-test-group-" + UUID.randomUUID();

        MarketEvent event = new MarketEvent(
                UUID.randomUUID(),
                "AAPL",
                new BigDecimal("150.2500"),
                1000,
                Instant.now()
        );

        String json = objectMapper.writeValueAsString(event);

        producer.send(
                new ProducerRecord<>(
                        topic,
                        event.symbol(),
                        json
                )
        ).get();

        KafkaSource firstSource =
                new KafkaSource(
                        "localhost:9092",
                        topic,
                        groupId,
                        "earliest"
                );

        firstSource.start();

        MarketEvent firstDelivery =
                waitForEventWithId(
                        firstSource,
                        event.eventId()
                );

        assertThat(firstDelivery)
                .isEqualTo(event);

        firstSource.commit();

        firstSource.stop();

        KafkaSource restartedSource =
                new KafkaSource(
                        "localhost:9092",
                        topic,
                        groupId,
                        "earliest"
                );

        try {
            restartedSource.start();

            boolean eventWasRedelivered = false;

            long deadline =
                    System.currentTimeMillis() + 3000;

            while (System.currentTimeMillis() < deadline) {

                MarketEvent received =
                        restartedSource.poll();

                if (received != null
                        && received.eventId().equals(event.eventId())) {

                    eventWasRedelivered = true;
                    break;
                }
            }

            assertThat(eventWasRedelivered)
                    .isFalse();

        } finally {
            restartedSource.stop();
        }
    }

    private MarketEvent waitForEventWithId(
            KafkaSource source,
            UUID eventId
    ) {

        long deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline) {

            MarketEvent event = source.poll();

            if (event != null && event.eventId().equals(eventId)) {
                return event;
            }
        }

        throw new AssertionError(
                "Timed out waiting for event with ID: " + eventId
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