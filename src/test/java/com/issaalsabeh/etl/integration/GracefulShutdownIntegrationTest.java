package com.issaalsabeh.etl.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.issaalsabeh.etl.connector.kafka.KafkaSource;
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
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class GracefulShutdownIntegrationTest {

    private static final String BOOTSTRAP_SERVERS =
            "localhost:9092";

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(
                            SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
                    );

    @Test
    void shouldFinishInFlightEventAndResumeFromNextEventAfterRestart()
            throws Exception {

        String topic =
                "graceful-shutdown-test-" + UUID.randomUUID();

        String groupId =
                "graceful-shutdown-group-" + UUID.randomUUID();

        MarketEvent firstEvent =
                createEvent(
                        "AAPL",
                        new BigDecimal("150.2500"),
                        1000
                );

        MarketEvent secondEvent =
                createEvent(
                        "AAPL",
                        new BigDecimal("151.5000"),
                        2000
                );

        publish(topic, firstEvent);
        publish(topic, secondEvent);

        KafkaSource source =
                new KafkaSource(
                        BOOTSTRAP_SERVERS,
                        topic,
                        groupId,
                        "earliest"
                );

        BlockingEnrichedSink sink =
                new BlockingEnrichedSink();

        Pipeline<MarketEvent> pipeline =
                new Pipeline<>(source);

        pipeline.addTransformer(
                new ValidationTransformer()
        );

        pipeline.addTransformer(
                new PriceNormalizationTransformer()
        );

        pipeline.addTransformer(
                new EnrichmentTransformer()
        );

        pipeline.addSink(sink);

        PipelineExecutor<MarketEvent> executor =
                new PipelineExecutor<>(pipeline);

        AtomicReference<Throwable> executorFailure =
                new AtomicReference<>();

        Thread executorThread =
                new Thread(() -> {

                    try {
                        executor.start();
                    } catch (Throwable e) {
                        executorFailure.set(e);
                    }

                });

        KafkaSource restartedSource = null;

        try {

            executorThread.start();

            /*
             * Wait until the first event is actually inside
             * sink.write().
             */
            assertThat(
                    sink.awaitFirstWriteStarted(
                            10,
                            TimeUnit.SECONDS
                    )
            )
                    .isTrue();

            /*
             * Request shutdown while the first event
             * is still in flight.
             */
            executor.stop();

            /*
             * The executor must not immediately die,
             * because the current event is still being handled.
             */
            assertThat(executorThread.isAlive())
                    .isTrue();

            /*
             * Allow the first sink write to complete.
             */
            sink.allowFirstWriteToFinish();

            executorThread.join(5_000);

            /*
             * The executor should now finish gracefully.
             */
            assertThat(executorThread.isAlive())
                    .isFalse();

            assertThat(executorFailure.get())
                    .isNull();

            /*
             * Only the first event should have been processed.
             * Shutdown was requested before another event
             * could begin.
             */
            assertThat(sink.getReceived())
                    .hasSize(1);

            assertThat(
                    sink.getReceived()
                            .get(0)
                            .eventId()
            )
                    .isEqualTo(firstEvent.eventId());

            /*
             * cleanupResources() should have stopped
             * the sink.
             */
            assertThat(sink.isStopped())
                    .isTrue();

            /*
             * Restart KafkaSource using the SAME topic
             * and SAME consumer group.
             *
             * If graceful shutdown committed the first
             * event correctly, Kafka should resume from
             * the second event.
             */
            restartedSource =
                    new KafkaSource(
                            BOOTSTRAP_SERVERS,
                            topic,
                            groupId,
                            "earliest"
                    );

            restartedSource.start();

            MarketEvent nextEvent =
                    waitForNextEvent(
                            restartedSource,
                            Duration.ofSeconds(10)
                    );

            assertThat(nextEvent)
                    .isNotNull();

            /*
             * This proves:
             *
             * first event  -> processed + committed
             * second event -> not processed before shutdown
             *                 and therefore available after restart
             */
            assertThat(nextEvent.eventId())
                    .isEqualTo(secondEvent.eventId());

        } finally {

            /*
             * Prevent the test from leaving the executor blocked
             * if an assertion fails before the latch is released.
             */
            sink.allowFirstWriteToFinish();

            executor.stop();

            executorThread.join(5_000);

            if (restartedSource != null) {
                restartedSource.stop();
            }
        }
    }

    private static MarketEvent createEvent(
            String symbol,
            BigDecimal price,
            long volume
    ) {

        return new MarketEvent(
                UUID.randomUUID(),
                symbol,
                price,
                volume,
                Instant.now()
        );
    }

    private static void publish(
            String topic,
            MarketEvent event
    ) throws Exception {

        Properties properties = new Properties();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS
        );

        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        properties.put(
                ProducerConfig.ACKS_CONFIG,
                "all"
        );

        try (
                KafkaProducer<String, String> producer =
                        new KafkaProducer<>(properties)
        ) {

            String json =
                    OBJECT_MAPPER.writeValueAsString(event);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            topic,
                            event.symbol(),
                            json
                    );

            producer.send(record).get();
        }
    }

    private static MarketEvent waitForNextEvent(
            KafkaSource source,
            Duration timeout
    ) {

        long deadline =
                System.nanoTime() + timeout.toNanos();

        while (System.nanoTime() < deadline) {

            MarketEvent event = source.poll();

            if (event != null) {
                return event;
            }
        }

        return null;
    }

    private static class BlockingEnrichedSink
            implements Sink<EnrichedMarketEvent> {

        private final CountDownLatch firstWriteStarted =
                new CountDownLatch(1);

        private final CountDownLatch allowFirstWriteToFinish =
                new CountDownLatch(1);

        private final List<EnrichedMarketEvent> received =
                new CopyOnWriteArrayList<>();

        private volatile boolean started;
        private volatile boolean stopped;

        @Override
        public void start() {
            started = true;
        }

        @Override
        public void write(EnrichedMarketEvent data) {

            if (!started) {
                throw new IllegalStateException(
                        "Sink has not been started"
                );
            }

            /*
             * Only block the first event.
             */
            if (received.isEmpty()) {

                firstWriteStarted.countDown();

                try {

                    allowFirstWriteToFinish.await();

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();

                    throw new IllegalStateException(
                            "Interrupted while waiting",
                            e
                    );
                }
            }

            received.add(data);
        }

        @Override
        public void stop() {
            stopped = true;
            started = false;
        }

        @Override
        public Class<?> getInputType() {
            return EnrichedMarketEvent.class;
        }

        boolean awaitFirstWriteStarted(
                long timeout,
                TimeUnit unit
        ) throws InterruptedException {

            return firstWriteStarted.await(
                    timeout,
                    unit
            );
        }

        void allowFirstWriteToFinish() {
            allowFirstWriteToFinish.countDown();
        }

        List<EnrichedMarketEvent> getReceived() {
            return received;
        }

        boolean isStopped() {
            return stopped;
        }
    }
}