package com.issaalsabeh.etl.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.issaalsabeh.etl.connector.kafka.KafkaSource;
import com.issaalsabeh.etl.connector.postgres.PostgresSink;
import com.issaalsabeh.etl.core.Pipeline;
import com.issaalsabeh.etl.core.PipelineExecutor;
import com.issaalsabeh.etl.core.Sink;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import com.issaalsabeh.etl.model.MarketEvent;
import com.issaalsabeh.etl.transformations.EnrichmentTransformer;
import com.issaalsabeh.etl.transformations.PriceNormalizationTransformer;
import com.issaalsabeh.etl.transformations.ValidationTransformer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import java.time.Instant;

import java.util.Properties;
import java.util.UUID;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaToPostgresIdempotencyIntegrationTest {

    private static final String KAFKA_BOOTSTRAP_SERVERS =
            "localhost:9092";

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16")
                    .withDatabaseName("market_data_test")
                    .withUsername("test_user")
                    .withPassword("test_password");

    @BeforeEach
    void setUp() throws Exception {

        try (
                Connection connection = DriverManager.getConnection(
                        POSTGRES.getJdbcUrl(),
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword()
                );

                Statement statement = connection.createStatement()
        ) {

            statement.execute("""
                    CREATE TABLE IF NOT EXISTS market_events (
                        event_id UUID PRIMARY KEY,
                        symbol VARCHAR(20) NOT NULL,
                        price NUMERIC(19,4) NOT NULL,
                        volume BIGINT NOT NULL,
                        timestamp TIMESTAMPTZ NOT NULL,
                        notional_value NUMERIC NOT NULL,
                        processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """);

            statement.execute(
                    "DELETE FROM market_events"
            );
        }
    }

    @Test
    void shouldStoreOnlyOneRecordWhenSameKafkaEventIsDeliveredTwice()
            throws Exception {

        String topic =
                "market-data-idempotency-test-"
                        + UUID.randomUUID();

        String groupId =
                "market-data-idempotency-group-"
                        + UUID.randomUUID();

        UUID eventId =
                UUID.randomUUID();

        MarketEvent event =
                new MarketEvent(
                        eventId,
                        "AAPL",
                        new BigDecimal("150.2500"),
                        1000L,
                        Instant.parse(
                                "2026-08-31T00:00:00Z"
                        )
                );

        /*
         * Publish the exact same logical event twice.
         *
         * Kafka now contains two records with the same eventId.
         */
        publishEventTwice(
                topic,
                event
        );

        KafkaSource source =
                new KafkaSource(
                        KAFKA_BOOTSTRAP_SERVERS,
                        topic,
                        groupId,
                        "earliest"
                );

        PostgresSink postgresSink =
                createPostgresSink();

        CountingPostgresSink countingSink =
                new CountingPostgresSink(
                        postgresSink,
                        2
                );

        Pipeline<MarketEvent> pipeline =
                Pipeline.<MarketEvent>builder()
                        .source(source)
                        .transform(
                                new ValidationTransformer()
                        )
                        .transform(
                                new PriceNormalizationTransformer()
                        )
                        .transform(
                                new EnrichmentTransformer()
                        )
                        .sink(countingSink)
                        .build();

        PipelineExecutor<MarketEvent> executor =
                new PipelineExecutor<>(
                        pipeline
                );

        AtomicReference<Throwable> executorFailure =
                new AtomicReference<>();

        Thread executorThread =
                new Thread(() -> {

                    try {
                        executor.start();

                    } catch (Throwable throwable) {
                        executorFailure.set(
                                throwable
                        );
                    }
                });

        executorThread.start();

        try {

            boolean processedBothDeliveries =
                    countingSink.awaitWrites(
                            15,
                            TimeUnit.SECONDS
                    );

            assertThat(
                    processedBothDeliveries
            )
                    .as(
                            "Both duplicate Kafka deliveries should reach Postgres"
                    )
                    .isTrue();

        } finally {

            executor.stop();

            executorThread.join(
                    5_000
            );
        }

        assertThat(
                executorFailure.get()
        )
                .as(
                        "Pipeline executor should not fail"
                )
                .isNull();

        assertThat(
                executorThread.isAlive()
        )
                .as(
                        "Pipeline executor should stop cleanly"
                )
                .isFalse();

        /*
         * Kafka delivered the event twice,
         * but Postgres should contain only one row
         * because event_id is the primary key and
         * PostgresSink uses ON CONFLICT DO NOTHING.
         */
        int recordCount =
                countRecordsByEventId(
                        eventId
                );

        assertThat(
                recordCount
        )
                .isEqualTo(1);
    }

    @Test
    void shouldRemainIdempotentWhenKafkaRedeliversAfterUncommittedRestart()
            throws Exception {

        String topic =
                "market-data-restart-test-"
                        + UUID.randomUUID();

        String groupId =
                "market-data-restart-group-"
                        + UUID.randomUUID();

        UUID eventId =
                UUID.randomUUID();

        MarketEvent event =
                new MarketEvent(
                        eventId,
                        "AAPL",
                        new BigDecimal("150.2500"),
                        1000L,
                        Instant.parse(
                                "2026-08-31T00:00:00Z"
                        )
                );

        /*
         * Only ONE Kafka record is published.
         *
         * The duplicate delivery later must therefore
         * come from Kafka redelivery after the missing commit.
         */
        publishEventOnce(
                topic,
                event
        );

        PostgresSink postgresSink =
                createPostgresSink();

        postgresSink.start();

        /*
         * ------------------------------------------------
         * First consumer
         * ------------------------------------------------
         *
         * Read and process the event successfully,
         * but intentionally DO NOT commit the Kafka offset.
         *
         * This simulates:
         *
         * Kafka poll
         *      ↓
         * processing succeeds
         *      ↓
         * Postgres write succeeds
         *      ↓
         * application crashes BEFORE offset commit
         */
        KafkaSource firstSource =
                new KafkaSource(
                        KAFKA_BOOTSTRAP_SERVERS,
                        topic,
                        groupId,
                        "earliest"
                );

        try {

            firstSource.start();

            MarketEvent firstDelivery =
                    waitForEventWithId(
                            firstSource,
                            eventId
                    );

            assertThat(
                    firstDelivery
            )
                    .isEqualTo(event);

            MarketEvent validated =
                    new ValidationTransformer()
                            .transform(
                                    firstDelivery
                            );

            MarketEvent normalized =
                    new PriceNormalizationTransformer()
                            .transform(
                                    validated
                            );

            EnrichedMarketEvent enriched =
                    new EnrichmentTransformer()
                            .transform(
                                    normalized
                            );

            postgresSink.write(
                    enriched
            );

            /*
             * DO NOT CALL:
             *
             * firstSource.commit();
             *
             * The missing commit is intentional.
             */

        } finally {

            firstSource.stop();
        }

        /*
         * At this point the database contains the event,
         * but Kafka still has no committed offset for it.
         */
        assertThat(
                countRecordsByEventId(
                        eventId
                )
        )
                .isEqualTo(1);

        /*
         * ------------------------------------------------
         * Restart consumer
         * ------------------------------------------------
         *
         * SAME topic.
         * SAME consumer group.
         *
         * Because the first consumer never committed,
         * Kafka should deliver the event again.
         */
        KafkaSource restartedSource =
                new KafkaSource(
                        KAFKA_BOOTSTRAP_SERVERS,
                        topic,
                        groupId,
                        "earliest"
                );

        try {

            restartedSource.start();

            MarketEvent redeliveredEvent =
                    waitForEventWithId(
                            restartedSource,
                            eventId
                    );

            assertThat(
                    redeliveredEvent
            )
                    .as(
                            "Kafka should redeliver the event because the first consumer never committed it"
                    )
                    .isEqualTo(event);

            MarketEvent validated =
                    new ValidationTransformer()
                            .transform(
                                    redeliveredEvent
                            );

            MarketEvent normalized =
                    new PriceNormalizationTransformer()
                            .transform(
                                    validated
                            );

            EnrichedMarketEvent enriched =
                    new EnrichmentTransformer()
                            .transform(
                                    normalized
                            );

            /*
             * Write the exact same logical event again.
             *
             * PostgresSink should absorb the duplicate using:
             *
             * ON CONFLICT (event_id) DO NOTHING
             */
            postgresSink.write(
                    enriched
            );

            /*
             * This second processing attempt finished
             * successfully, so now commit the Kafka offset.
             */
            restartedSource.commit();

        } finally {

            restartedSource.stop();
            postgresSink.stop();
        }

        /*
         * Kafka delivered the same event twice:
         *
         * first delivery  → write succeeds, no commit
         * restart         → Kafka redelivers
         * second delivery → write succeeds, commit
         *
         * Postgres must still contain exactly one row.
         */
        int recordCount =
                countRecordsByEventId(
                        eventId
                );

        assertThat(
                recordCount
        )
                .as(
                        "Kafka redelivery must not create duplicate Postgres rows"
                )
                .isEqualTo(1);
    }

    private void publishEventOnce(
            String topic,
            MarketEvent event
    ) throws Exception {

        Properties properties =
                createProducerProperties();

        ObjectMapper objectMapper =
                new ObjectMapper();

        objectMapper.registerModule(
                new JavaTimeModule()
        );

        String json =
                objectMapper.writeValueAsString(
                        event
                );

        try (
                KafkaProducer<String, String> producer =
                        new KafkaProducer<>(
                                properties
                        )
        ) {

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            topic,
                            event.symbol(),
                            json
                    );

            producer.send(
                    record
            ).get();

            producer.flush();
        }
    }

    private void publishEventTwice(
            String topic,
            MarketEvent event
    ) throws Exception {

        Properties properties =
                createProducerProperties();

        ObjectMapper objectMapper =
                new ObjectMapper();

        objectMapper.registerModule(
                new JavaTimeModule()
        );

        String json =
                objectMapper.writeValueAsString(
                        event
                );

        try (
                KafkaProducer<String, String> producer =
                        new KafkaProducer<>(
                                properties
                        )
        ) {

            ProducerRecord<String, String> firstRecord =
                    new ProducerRecord<>(
                            topic,
                            event.symbol(),
                            json
                    );

            ProducerRecord<String, String> secondRecord =
                    new ProducerRecord<>(
                            topic,
                            event.symbol(),
                            json
                    );

            producer.send(
                    firstRecord
            ).get();

            producer.send(
                    secondRecord
            ).get();

            producer.flush();
        }
    }

    private Properties createProducerProperties() {

        Properties properties =
                new Properties();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_BOOTSTRAP_SERVERS
        );

        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        return properties;
    }

    private MarketEvent waitForEventWithId(
            KafkaSource source,
            UUID eventId
    ) throws InterruptedException {

        long deadline =
                System.currentTimeMillis()
                        + 10_000;

        while (
                System.currentTimeMillis()
                        < deadline
        ) {

            MarketEvent event =
                    source.poll();

            if (
                    event != null
                            && event.eventId()
                            .equals(eventId)
            ) {

                return event;
            }

            Thread.sleep(
                    10
            );
        }

        throw new AssertionError(
                "Timed out waiting for Kafka event with id "
                        + eventId
        );
    }

    private int countRecordsByEventId(
            UUID eventId
    ) throws Exception {

        try (
                Connection connection =
                        DriverManager.getConnection(
                                POSTGRES.getJdbcUrl(),
                                POSTGRES.getUsername(),
                                POSTGRES.getPassword()
                        );

                PreparedStatement statement =
                        connection.prepareStatement(
                                """
                                SELECT COUNT(*)
                                FROM market_events
                                WHERE event_id = ?
                                """
                        )
        ) {

            statement.setObject(
                    1,
                    eventId
            );

            try (
                    ResultSet resultSet =
                            statement.executeQuery()
            ) {

                assertThat(
                        resultSet.next()
                )
                        .isTrue();

                return resultSet.getInt(
                        1
                );
            }
        }
    }

    private PostgresSink createPostgresSink() {

        return new PostgresSink(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword()
        );
    }

    private static class CountingPostgresSink
            implements Sink<EnrichedMarketEvent> {

        private final PostgresSink delegate;

        private final CountDownLatch writeLatch;

        private CountingPostgresSink(
                PostgresSink delegate,
                int expectedWrites
        ) {

            this.delegate =
                    delegate;

            this.writeLatch =
                    new CountDownLatch(
                            expectedWrites
                    );
        }

        @Override
        public void start() {

            delegate.start();
        }

        @Override
        public void write(
                EnrichedMarketEvent event
        ) {

            delegate.write(
                    event
            );

            writeLatch.countDown();
        }

        @Override
        public void stop() {

            delegate.stop();
        }

        @Override
        public Class<?> getInputType() {

            return EnrichedMarketEvent.class;
        }

        private boolean awaitWrites(
                long timeout,
                TimeUnit unit
        ) throws InterruptedException {

            return writeLatch.await(
                    timeout,
                    unit
            );
        }
    }
}