package com.issaalsabeh.etl.core;

import com.issaalsabeh.etl.core.dlq.DeadLetterQueue;
import com.issaalsabeh.etl.core.dlq.DeadLetterRecord;
import com.issaalsabeh.etl.core.dlq.NoOpDeadLetterQueue;
import com.issaalsabeh.etl.core.retry.RetryPolicy;
import com.issaalsabeh.etl.monitoring.PipelineMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineExecutorMetricsTest {

    private static final RetryPolicy ZERO_DELAY_RETRY_POLICY =
            new RetryPolicy(
                    3,
                    0,
                    1.0,
                    0
            );

    @Test
    void shouldRecordSuccessfulEventAsProcessed()
            throws InterruptedException {

        SimpleMeterRegistry registry =
                new SimpleMeterRegistry();

        PipelineMetrics metrics =
                new PipelineMetrics(
                        registry,
                        "test-pipeline"
                );

        SingleEventSource source =
                new SingleEventSource("hello");

        RecordingSink sink =
                new RecordingSink();

        Pipeline<String> pipeline =
                Pipeline.<String>builder()
                        .name("test-pipeline")
                        .source(source)
                        .sink(sink)
                        .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(
                        pipeline,
                        ZERO_DELAY_RETRY_POLICY,
                        new NoOpDeadLetterQueue(),
                        metrics
                );

        Thread executorThread =
                new Thread(executor::start);

        executorThread.start();

        assertThat(
                sink.awaitWrite()
        ).isTrue();

        awaitMetric(
                registry,
                "pipeline.events.processed",
                1.0
        );

        executor.stop();

        executorThread.join(2_000);

        assertThat(executorThread.isAlive())
                .isFalse();

        assertMetric(
                registry,
                "pipeline.events.received",
                1.0
        );

        assertMetric(
                registry,
                "pipeline.events.processed",
                1.0
        );

        assertMetric(
                registry,
                "pipeline.events.failed",
                0.0
        );

        assertMetric(
                registry,
                "pipeline.events.retried",
                0.0
        );

        assertMetric(
                registry,
                "pipeline.events.dlq",
                0.0
        );
    }

    @Test
    void shouldRecordRetriedEventAsProcessedWhenRetrySucceeds()
            throws InterruptedException {

        SimpleMeterRegistry registry =
                new SimpleMeterRegistry();

        PipelineMetrics metrics =
                new PipelineMetrics(
                        registry,
                        "test-pipeline"
                );

        SingleEventSource source =
                new SingleEventSource("hello");

        RetryOnceSink sink =
                new RetryOnceSink();

        Pipeline<String> pipeline =
                Pipeline.<String>builder()
                        .name("test-pipeline")
                        .source(source)
                        .sink(sink)
                        .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(
                        pipeline,
                        ZERO_DELAY_RETRY_POLICY,
                        new NoOpDeadLetterQueue(),
                        metrics
                );

        Thread executorThread =
                new Thread(executor::start);

        executorThread.start();

        assertThat(
                sink.awaitSuccessfulWrite()
        ).isTrue();

        awaitMetric(
                registry,
                "pipeline.events.retried",
                1.0
        );

        awaitMetric(
                registry,
                "pipeline.events.processed",
                1.0
        );

        executor.stop();

        executorThread.join(2_000);

        assertThat(executorThread.isAlive())
                .isFalse();

        assertThat(sink.getAttempts())
                .isEqualTo(2);

        assertMetric(
                registry,
                "pipeline.events.received",
                1.0
        );

        assertMetric(
                registry,
                "pipeline.events.processed",
                1.0
        );

        assertMetric(
                registry,
                "pipeline.events.failed",
                0.0
        );

        assertMetric(
                registry,
                "pipeline.events.retried",
                1.0
        );

        assertMetric(
                registry,
                "pipeline.events.dlq",
                0.0
        );
    }

    @Test
    void shouldRecordFailedEventWhenPermanentlyFailedEventIsDeadLettered()
            throws InterruptedException {

        SimpleMeterRegistry registry =
                new SimpleMeterRegistry();

        PipelineMetrics metrics =
                new PipelineMetrics(
                        registry,
                        "test-pipeline"
                );

        SingleEventSource source =
                new SingleEventSource("hello");

        AlwaysFailingSink sink =
                new AlwaysFailingSink();

        RecordingDeadLetterQueue deadLetterQueue =
                new RecordingDeadLetterQueue();

        Pipeline<String> pipeline =
                Pipeline.<String>builder()
                        .name("test-pipeline")
                        .source(source)
                        .sink(sink)
                        .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(
                        pipeline,
                        ZERO_DELAY_RETRY_POLICY,
                        deadLetterQueue,
                        metrics
                );

        Thread executorThread =
                new Thread(executor::start);

        executorThread.start();

        assertThat(
                deadLetterQueue.awaitPublish()
        ).isTrue();

        awaitMetric(
                registry,
                "pipeline.events.failed",
                1.0
        );

        awaitMetric(
                registry,
                "pipeline.events.dlq",
                1.0
        );

        executor.stop();

        executorThread.join(2_000);

        assertThat(executorThread.isAlive())
                .isFalse();

        assertThat(sink.getAttempts())
                .isEqualTo(3);

        assertMetric(
                registry,
                "pipeline.events.received",
                1.0
        );

        assertMetric(
                registry,
                "pipeline.events.processed",
                0.0
        );

        assertMetric(
                registry,
                "pipeline.events.failed",
                1.0
        );

        assertMetric(
                registry,
                "pipeline.events.retried",
                1.0
        );

        assertMetric(
                registry,
                "pipeline.events.dlq",
                1.0
        );
    }

    @Test
    void shouldRecordTransformationFailureAsFailed()
            throws InterruptedException {

        SimpleMeterRegistry registry =
                new SimpleMeterRegistry();

        PipelineMetrics metrics =
                new PipelineMetrics(
                        registry,
                        "test-pipeline"
                );

        SingleEventSource source =
                new SingleEventSource("bad");

        FailingTransformer transformer =
                new FailingTransformer();

        RecordingSink sink =
                new RecordingSink();

        Pipeline<String> pipeline =
                Pipeline.<String>builder()
                        .name("test-pipeline")
                        .source(source)
                        .transform(transformer)
                        .sink(sink)
                        .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(
                        pipeline,
                        ZERO_DELAY_RETRY_POLICY,
                        new NoOpDeadLetterQueue(),
                        metrics
                );

        Thread executorThread =
                new Thread(executor::start);

        executorThread.start();

        assertThat(
                transformer.awaitAttempt()
        ).isTrue();

        awaitMetric(
                registry,
                "pipeline.events.failed",
                1.0
        );

        executor.stop();

        executorThread.join(2_000);

        assertThat(executorThread.isAlive())
                .isFalse();

        assertMetric(
                registry,
                "pipeline.events.received",
                1.0
        );

        assertMetric(
                registry,
                "pipeline.events.processed",
                0.0
        );

        assertMetric(
                registry,
                "pipeline.events.failed",
                1.0
        );

        assertMetric(
                registry,
                "pipeline.events.retried",
                0.0
        );

        assertMetric(
                registry,
                "pipeline.events.dlq",
                0.0
        );

        assertThat(sink.getWriteCount())
                .isZero();
    }

    private static void assertMetric(
            SimpleMeterRegistry registry,
            String metricName,
            double expected
    ) {

        assertThat(
                registry.get(metricName)
                        .tag(
                                "pipeline",
                                "test-pipeline"
                        )
                        .counter()
                        .count()
        ).isEqualTo(expected);
    }

    private static void awaitMetric(
            SimpleMeterRegistry registry,
            String metricName,
            double expected
    ) throws InterruptedException {

        long deadline =
                System.currentTimeMillis() + 2_000;

        while (System.currentTimeMillis() < deadline) {

            double current =
                    registry.get(metricName)
                            .tag(
                                    "pipeline",
                                    "test-pipeline"
                            )
                            .counter()
                            .count();

            if (current == expected) {
                return;
            }

            Thread.sleep(10);
        }

        assertMetric(
                registry,
                metricName,
                expected
        );
    }

    private static class SingleEventSource
            implements Source<String> {

        private final String event;

        private boolean started;
        private boolean emitted;

        private SingleEventSource(String event) {
            this.event = event;
        }

        @Override
        public void start() {
            started = true;
        }

        @Override
        public String poll() {

            if (!started) {
                throw new IllegalStateException(
                        "Source not started"
                );
            }

            if (emitted) {
                return null;
            }

            emitted = true;

            return event;
        }

        @Override
        public void stop() {
            started = false;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    }

    private static class RecordingSink
            implements Sink<String> {

        private final AtomicInteger writeCount =
                new AtomicInteger();

        private final CountDownLatch written =
                new CountDownLatch(1);

        @Override
        public void start() {
        }

        @Override
        public void write(String data) {

            writeCount.incrementAndGet();

            written.countDown();
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        private boolean awaitWrite()
                throws InterruptedException {

            return written.await(
                    2,
                    TimeUnit.SECONDS
            );
        }

        private int getWriteCount() {
            return writeCount.get();
        }
    }

    private static class RetryOnceSink
            implements Sink<String> {

        private final AtomicInteger attempts =
                new AtomicInteger();

        private final CountDownLatch successfulWrite =
                new CountDownLatch(1);

        @Override
        public void start() {
        }

        @Override
        public void write(String data) {

            int attempt =
                    attempts.incrementAndGet();

            if (attempt == 1) {
                throw new RuntimeException(
                        "Temporary failure"
                );
            }

            successfulWrite.countDown();
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        private boolean awaitSuccessfulWrite()
                throws InterruptedException {

            return successfulWrite.await(
                    2,
                    TimeUnit.SECONDS
            );
        }

        private int getAttempts() {
            return attempts.get();
        }
    }

    private static class AlwaysFailingSink
            implements Sink<String> {

        private final AtomicInteger attempts =
                new AtomicInteger();

        @Override
        public void start() {
        }

        @Override
        public void write(String data) {

            attempts.incrementAndGet();

            throw new RuntimeException(
                    "Permanent failure"
            );
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        private int getAttempts() {
            return attempts.get();
        }
    }

    private static class RecordingDeadLetterQueue
            implements DeadLetterQueue {

        private final CountDownLatch published =
                new CountDownLatch(1);

        @Override
        public void start() {
        }

        @Override
        public void publish(
                DeadLetterRecord record
        ) {

            published.countDown();
        }

        @Override
        public void stop() {
        }

        private boolean awaitPublish()
                throws InterruptedException {

            return published.await(
                    2,
                    TimeUnit.SECONDS
            );
        }
    }

    private static class FailingTransformer
            implements Transformer<String, String> {

        private final CountDownLatch attempted =
                new CountDownLatch(1);

        @Override
        public String transform(String input) {

            attempted.countDown();

            throw new IllegalArgumentException(
                    "Invalid event"
            );
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }

        private boolean awaitAttempt()
                throws InterruptedException {

            return attempted.await(
                    2,
                    TimeUnit.SECONDS
            );
        }
    }
}
