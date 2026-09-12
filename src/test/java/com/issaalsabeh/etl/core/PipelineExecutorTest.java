package com.issaalsabeh.etl.core;

import com.issaalsabeh.etl.core.dlq.DeadLetterQueue;
import com.issaalsabeh.etl.core.dlq.DeadLetterRecord;
import com.issaalsabeh.etl.core.retry.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

class PipelineExecutorTest {

    @Test
    void shouldRejectNullPipeline() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PipelineExecutor<String>(null)
        );
    }

    @Test
    void shouldStartSourceAndSink() throws InterruptedException {
        TestSource source = new TestSource("hello");
        RecordingSink sink = new RecordingSink(1);

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .sink(sink)
                .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        Thread thread = new Thread(executor::start);
        thread.start();

        assertTrue(sink.awaitEvent());

        executor.stop();
        thread.join(1000);

        assertTrue(source.started);
        assertTrue(sink.started);
    }

    @Test
    void shouldProcessEventThroughTransformers() throws InterruptedException {
        TestSource source = new TestSource(" hello ");
        RecordingSink sink = new RecordingSink(1);

        Transformer<String, String> trimTransformer =
                new TrimTransformer();

        Transformer<String, String> upperCaseTransformer =
                new UpperCaseTransformer();

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .transform(trimTransformer)
                .transform(upperCaseTransformer)
                .sink(sink)
                .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        Thread thread = new Thread(executor::start);
        thread.start();

        assertTrue(sink.awaitEvent());

        executor.stop();
        thread.join(1000);

        assertEquals(1, sink.received.size());
        assertEquals("HELLO", sink.received.get(0));
    }

    @Test
    void shouldExecuteTransformersInOrder() throws InterruptedException {
        TestSource source = new TestSource("start");
        RecordingSink sink = new RecordingSink(1);

        Transformer<String, String> first =
                new AppendTransformer("-A");

        Transformer<String, String> second =
                new AppendTransformer("-B");

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .transform(first)
                .transform(second)
                .sink(sink)
                .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        Thread thread = new Thread(executor::start);
        thread.start();

        assertTrue(sink.awaitEvent());

        executor.stop();
        thread.join(1000);

        assertEquals("start-A-B", sink.received.get(0));
    }

    @Test
    void shouldSkipFailedTransformationAndContinueProcessing()
            throws InterruptedException {

        TestSource source = new TestSource("bad", "good");
        RecordingSink sink = new RecordingSink(1);

        Transformer<String, String> transformer =
                new FailingOnBadTransformer();

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .transform(transformer)
                .sink(sink)
                .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        Thread thread = new Thread(executor::start);
        thread.start();

        assertTrue(sink.awaitEvent());

        executor.stop();
        thread.join(1000);

        assertEquals(1, sink.received.size());
        assertEquals("GOOD", sink.received.get(0));
    }

    @Test
    void shouldContinueToOtherSinksWhenOneSinkFails()
            throws InterruptedException {

        TestSource source = new TestSource("hello");

        FailingSink failingSink = new FailingSink();
        RecordingSink workingSink = new RecordingSink(1);

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .sink(failingSink)
                .sink(workingSink)
                .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        Thread thread = new Thread(executor::start);
        thread.start();

        assertTrue(workingSink.awaitEvent());

        executor.stop();
        thread.join(1000);

        assertEquals(3, failingSink.writeAttempts.get());
        assertEquals("hello", workingSink.received.get(0));
    }

    @Test
    void shouldStopSourceAndSinksOnCleanShutdown()
            throws InterruptedException {

        TestSource source = new TestSource("hello");
        RecordingSink sink = new RecordingSink(1);

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .sink(sink)
                .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        Thread thread = new Thread(executor::start);
        thread.start();

        assertTrue(sink.awaitEvent());

        executor.stop();
        thread.join(1000);

        assertFalse(thread.isAlive());

        assertTrue(source.stopped);
        assertTrue(sink.stopped);
    }

    @Test
    void shouldCleanupWhenPollThrowsException() {
        FailingSource source = new FailingSource();
        RecordingSink sink = new RecordingSink(0);

        Pipeline<String> pipeline = Pipeline.<String>builder()
                .source(source)
                .sink(sink)
                .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        assertThrows(
                IllegalStateException.class,
                executor::start
        );

        assertTrue(source.stopped);
        assertTrue(sink.stopped);
    }

    @Test
    void shouldRetrySinkUntilItSucceeds() throws InterruptedException {

        SingleEventSource source =
                new SingleEventSource();

        RetryableFailingSink sink =
                new RetryableFailingSink(2);

        Pipeline<String> pipeline =
                Pipeline.<String>builder()
                        .source(source)
                        .sink(sink)
                        .build();

        RetryPolicy retryPolicy =
                new RetryPolicy(
                        3,
                        0,
                        2.0,
                        0
                );

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(
                        pipeline,
                        retryPolicy
                );

        Thread executorThread =
                new Thread(executor::start);

        executorThread.start();

        long deadline =
                System.currentTimeMillis() + 2_000;

        while (sink.getAttempts() < 3
                && System.currentTimeMillis() < deadline) {
            Thread.yield();
        }

        executor.stop();

        executorThread.join(2_000);

        assertThat(sink.getAttempts())
                .isEqualTo(3);

        assertThat(executorThread.isAlive())
                .isFalse();
    }

    @Test
    void shouldStopRetryingAfterMaximumAttempts()
            throws InterruptedException {

        SingleEventSource source =
                new SingleEventSource();

        RetryableFailingSink sink =
                new RetryableFailingSink(100);

        Pipeline<String> pipeline =
                Pipeline.<String>builder()
                        .source(source)
                        .sink(sink)
                        .build();

        RetryPolicy retryPolicy =
                new RetryPolicy(
                        3,
                        0,
                        2.0,
                        0
                );

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(
                        pipeline,
                        retryPolicy
                );

        Thread executorThread =
                new Thread(executor::start);

        executorThread.start();

        long deadline =
                System.currentTimeMillis() + 2_000;

        while (sink.getAttempts() < 3
                && System.currentTimeMillis() < deadline) {
            Thread.yield();
        }

        executor.stop();

        executorThread.join(2_000);

        assertThat(sink.getAttempts())
                .isEqualTo(3);

        assertThat(executorThread.isAlive())
                .isFalse();
    }

    @Test
    void shouldSendPermanentlyFailedEventToDlq()
            throws InterruptedException {

        TestSource source =
                new TestSource("hello");

        RetryableFailingSink failingSink =
                new RetryableFailingSink(100);

        RecordingDeadLetterQueue deadLetterQueue =
                new RecordingDeadLetterQueue();

        Pipeline<String> pipeline =
                Pipeline.<String>builder()
                        .source(source)
                        .sink(failingSink)
                        .build();

        RetryPolicy retryPolicy =
                new RetryPolicy(
                        3,
                        0,
                        2.0,
                        0
                );

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(
                        pipeline,
                        retryPolicy,
                        deadLetterQueue
                );

        Thread thread =
                new Thread(executor::start);

        thread.start();

        assertTrue(
                deadLetterQueue.awaitRecord()
        );

        executor.stop();
        thread.join(2_000);

        assertEquals(
                3,
                failingSink.getAttempts()
        );

        assertEquals(
                1,
                deadLetterQueue.getRecords().size()
        );

        DeadLetterRecord record =
                deadLetterQueue.getRecords().get(0);

        assertEquals(
                "hello",
                record.originalEvent()
        );

        assertEquals(
                "RetryableFailingSink",
                record.failedSink()
        );

        assertEquals(
                RuntimeException.class.getName(),
                record.errorType()
        );

        assertEquals(
                "Simulated sink failure",
                record.errorMessage()
        );

        assertNotNull(
                record.failureTimestamp()
        );

        assertEquals(
                2,
                record.retryCount()
        );
    }

    @Test
    void shouldNotSendRecoveredEventToDlq()
            throws InterruptedException {

        TestSource source =
                new TestSource("hello");

        RetryableFailingSink recoveringSink =
                new RetryableFailingSink(2);

        RecordingDeadLetterQueue deadLetterQueue =
                new RecordingDeadLetterQueue();

        Pipeline<String> pipeline =
                Pipeline.<String>builder()
                        .source(source)
                        .sink(recoveringSink)
                        .build();

        RetryPolicy retryPolicy =
                new RetryPolicy(
                        3,
                        0,
                        2.0,
                        0
                );

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(
                        pipeline,
                        retryPolicy,
                        deadLetterQueue
                );

        Thread thread =
                new Thread(executor::start);

        thread.start();

        long deadline =
                System.currentTimeMillis() + 2_000;

        while (recoveringSink.getAttempts() < 3
                && System.currentTimeMillis() < deadline) {
            Thread.yield();
        }

        executor.stop();
        thread.join(2_000);

        assertEquals(
                3,
                recoveringSink.getAttempts()
        );

        assertTrue(
                deadLetterQueue.getRecords().isEmpty()
        );
    }

    @Test
    void shouldCommitSourceAfterSuccessfulProcessing()
            throws InterruptedException {

        CommittableTestSource source =
                new CommittableTestSource("hello");

        RecordingSink sink =
                new RecordingSink(1);

        Pipeline<String> pipeline =
                Pipeline.<String>builder()
                        .source(source)
                        .sink(sink)
                        .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        Thread thread =
                new Thread(executor::start);

        thread.start();

        assertTrue(sink.awaitEvent());

        long deadline =
                System.currentTimeMillis() + 2_000;

        while (source.getCommitCount() < 1
                && System.currentTimeMillis() < deadline) {
            Thread.yield();
        }

        executor.stop();
        thread.join(2_000);

        assertThat(source.getCommitCount())
                .isEqualTo(1);
    }

    @Test
    void shouldNotCommitWhenEventRemainsUnresolved()
            throws InterruptedException {

        CommittableTestSource source =
                new CommittableTestSource("hello");

        RetryableFailingSink sink =
                new RetryableFailingSink(100);

        FailingDeadLetterQueue deadLetterQueue =
                new FailingDeadLetterQueue();

        Pipeline<String> pipeline =
                Pipeline.<String>builder()
                        .source(source)
                        .sink(sink)
                        .build();

        RetryPolicy retryPolicy =
                new RetryPolicy(
                        3,
                        0,
                        2.0,
                        0
                );

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(
                        pipeline,
                        retryPolicy,
                        deadLetterQueue
                );

        Thread thread =
                new Thread(executor::start);

        thread.start();
        thread.join(2_000);

        assertThat(source.getCommitCount())
                .isZero();

        assertThat(thread.isAlive())
                .isFalse();
    }

    @Test
    void shouldCommitRejectedTransformation()
            throws InterruptedException {

        CommittableTestSource source =
                new CommittableTestSource("bad");

        RecordingSink sink =
                new RecordingSink(0);

        Pipeline<String> pipeline =
                Pipeline.<String>builder()
                        .source(source)
                        .transform(new FailingOnBadTransformer())
                        .sink(sink)
                        .build();

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        Thread thread =
                new Thread(executor::start);

        thread.start();

        long deadline =
                System.currentTimeMillis() + 2_000;

        while (source.getCommitCount() < 1
                && System.currentTimeMillis() < deadline) {
            Thread.yield();
        }

        executor.stop();
        thread.join(2_000);

        assertThat(source.getCommitCount())
                .isEqualTo(1);

        assertThat(sink.received)
                .isEmpty();
    }

    @Test
    void shouldFinishInFlightEventBeforeShutdown() throws Exception {

        SingleEventSource source = new SingleEventSource();
        BlockingSink sink = new BlockingSink();

        Pipeline<String> pipeline = new Pipeline<>(source);
        pipeline.addSink(sink);

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        Thread executorThread = new Thread(executor::start);
        executorThread.start();

        assertThat(sink.awaitWriteStarted(2, TimeUnit.SECONDS))
                .isTrue();

        executor.stop();

        assertThat(executorThread.isAlive())
                .isTrue();

        sink.allowWriteToFinish();

        executorThread.join(2_000);

        assertThat(executorThread.isAlive())
                .isFalse();

        assertThat(sink.getReceived())
                .containsExactly("test-event");
    }

    @Test
    void shouldCommitInFlightEventBeforeGracefulShutdown() throws Exception {

        CommittableSingleEventSource source =
                new CommittableSingleEventSource("hello");

        BlockingSink sink = new BlockingSink();

        Pipeline<String> pipeline = new Pipeline<>(source);
        pipeline.addSink(sink);

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        Thread executorThread = new Thread(executor::start);
        executorThread.start();

        assertThat(sink.awaitWriteStarted(2, TimeUnit.SECONDS))
                .isTrue();

        executor.stop();

        sink.allowWriteToFinish();

        executorThread.join(2_000);

        assertThat(executorThread.isAlive())
                .isFalse();

        assertThat(source.getCommitCount())
                .isEqualTo(1);
    }

    @Test
    void shouldNotProcessAnotherEventAfterShutdownIsRequested() throws Exception {

        TwoEventSource source =
                new TwoEventSource("first", "second");

        BlockingFirstWriteSink sink =
                new BlockingFirstWriteSink();

        Pipeline<String> pipeline = new Pipeline<>(source);
        pipeline.addSink(sink);

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(pipeline);

        Thread executorThread = new Thread(executor::start);
        executorThread.start();

        assertThat(sink.awaitFirstWriteStarted(2, TimeUnit.SECONDS))
                .isTrue();

        executor.stop();

        sink.allowFirstWriteToFinish();

        executorThread.join(2_000);

        assertThat(executorThread.isAlive())
                .isFalse();

        assertThat(sink.getReceived())
                .containsExactly("first");
    }

    @Test
    void shouldContinueCleanupWhenOneSinkFailsToStop() throws Exception {

        RecordingStopSource source =
                new RecordingStopSource("hello");

        FailingStopSink failingSink =
                new FailingStopSink();

        RecordingStopSink workingSink =
                new RecordingStopSink();

        RecordingStopDeadLetterQueue deadLetterQueue =
                new RecordingStopDeadLetterQueue();

        Pipeline<String> pipeline = new Pipeline<>(source);
        pipeline.addSink(failingSink);
        pipeline.addSink(workingSink);

        PipelineExecutor<String> executor =
                new PipelineExecutor<>(
                        pipeline,
                        new RetryPolicy(1, 0, 1.0, 0),
                        deadLetterQueue
                );

        Thread executorThread = new Thread(executor::start);
        executorThread.start();

        assertThat(workingSink.awaitWrite(2, TimeUnit.SECONDS))
                .isTrue();

        executor.stop();

        executorThread.join(2_000);

        assertThat(executorThread.isAlive())
                .isFalse();

        assertThat(failingSink.wasStopCalled())
                .isTrue();

        assertThat(workingSink.isStopped())
                .isTrue();

        assertThat(deadLetterQueue.isStopped())
                .isTrue();

        assertThat(source.isStopped())
                .isTrue();
    }



    // ---------- Recording Stop Dead Letter Queue ----------

    private static class RecordingStopDeadLetterQueue
            implements DeadLetterQueue {

        private volatile boolean stopped;

        @Override
        public void start() {
        }

        @Override
        public void publish(DeadLetterRecord record) {
        }

        @Override
        public void stop() {
            stopped = true;
        }

        boolean isStopped() {
            return stopped;
        }
    }

    // ---------- Failing Stop Sink ----------

    private static class FailingStopSink
            implements Sink<String> {

        private volatile boolean stopCalled;

        @Override
        public void start() {
        }

        @Override
        public void write(String data) {
        }

        @Override
        public void stop() {
            stopCalled = true;
            throw new RuntimeException(
                    "Failed to stop sink"
            );
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        boolean wasStopCalled() {
            return stopCalled;
        }
    }

    // ---------- Recording Stop Sink ----------

    private static class RecordingStopSink
            implements Sink<String> {

        private final CountDownLatch writeLatch =
                new CountDownLatch(1);

        private volatile boolean stopped;

        @Override
        public void start() {
        }

        @Override
        public void write(String data) {
            writeLatch.countDown();
        }

        @Override
        public void stop() {
            stopped = true;
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        boolean awaitWrite(
                long timeout,
                TimeUnit unit
        ) throws InterruptedException {

            return writeLatch.await(timeout, unit);
        }

        boolean isStopped() {
            return stopped;
        }
    }

    // ---------- Recording Stop Source ----------

    private static class RecordingStopSource
            implements Source<String> {

        private final String event;

        private boolean emitted;
        private boolean running;
        private volatile boolean stopped;

        private RecordingStopSource(String event) {
            this.event = event;
        }

        @Override
        public void start() {
            running = true;
        }

        @Override
        public String poll() {

            if (!running) {
                throw new IllegalStateException(
                        "Source is not running"
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
            stopped = true;
            running = false;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }

        boolean isStopped() {
            return stopped;
        }
    }

    // ---------- Blocking first Write Sink ----------

    private static class BlockingFirstWriteSink
            implements Sink<String> {

        private final CountDownLatch firstWriteStarted =
                new CountDownLatch(1);

        private final CountDownLatch allowFirstWriteToFinish =
                new CountDownLatch(1);

        private final List<String> received =
                new ArrayList<>();

        @Override
        public void start() {
        }

        @Override
        public void write(String data) {

            if (received.isEmpty()) {

                firstWriteStarted.countDown();

                try {
                    allowFirstWriteToFinish.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }

            received.add(data);
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        boolean awaitFirstWriteStarted(
                long timeout,
                TimeUnit unit
        ) throws InterruptedException {

            return firstWriteStarted.await(timeout, unit);
        }

        void allowFirstWriteToFinish() {
            allowFirstWriteToFinish.countDown();
        }

        List<String> getReceived() {
            return received;
        }
    }

    // ---------- Two Event Source ----------

    private static class TwoEventSource implements Source<String> {

        private final Queue<String> events =
                new ArrayDeque<>();

        private boolean running;

        private TwoEventSource(String first, String second) {
            events.add(first);
            events.add(second);
        }

        @Override
        public void start() {
            running = true;
        }

        @Override
        public String poll() {

            if (!running) {
                throw new IllegalStateException(
                        "Source is not running"
                );
            }

            return events.poll();
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    }

    // ---------- Committable Single Event Source ----------

    private static class CommittableSingleEventSource
            implements CommittableSource<String> {

        private final String event;

        private final AtomicInteger commitCount =
                new AtomicInteger();

        private boolean running;
        private boolean emitted;

        private CommittableSingleEventSource(String event) {
            this.event = event;
        }

        @Override
        public void start() {
            running = true;
        }

        @Override
        public String poll() {

            if (!running) {
                throw new IllegalStateException(
                        "Source is not running"
                );
            }

            if (emitted) {
                return null;
            }

            emitted = true;
            return event;
        }

        @Override
        public void commit() {
            commitCount.incrementAndGet();
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }

        int getCommitCount() {
            return commitCount.get();
        }
    }

    // ---------- Blocking Sink ----------

    private static class BlockingSink implements Sink<String> {

        private final CountDownLatch writeStarted =
                new CountDownLatch(1);

        private final CountDownLatch allowWriteToFinish =
                new CountDownLatch(1);

        private final List<String> received =
                new ArrayList<>();

        @Override
        public void start() {
        }

        @Override
        public void write(String data) {

            writeStarted.countDown();

            try {
                allowWriteToFinish.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            received.add(data);
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        boolean awaitWriteStarted(
                long timeout,
                TimeUnit unit
        ) throws InterruptedException {

            return writeStarted.await(timeout, unit);
        }

        void allowWriteToFinish() {
            allowWriteToFinish.countDown();
        }

        List<String> getReceived() {
            return received;
        }
    }

    // ---------- Test Transformers ----------

    private static class TrimTransformer
            implements Transformer<String, String> {

        @Override
        public String transform(String input) {
            return input.trim();
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    }


    private static class UpperCaseTransformer
            implements Transformer<String, String> {

        @Override
        public String transform(String input) {
            return input.toUpperCase();
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    }


    private static class AppendTransformer
            implements Transformer<String, String> {

        private final String suffix;

        AppendTransformer(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public String transform(String input) {
            return input + suffix;
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    }


    private static class FailingOnBadTransformer
            implements Transformer<String, String> {

        @Override
        public String transform(String input) {
            if (input.equals("bad")) {
                throw new IllegalArgumentException("Bad event");
            }

            return input.toUpperCase();
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    }


    // ---------- Test Source ----------

    private static class TestSource implements Source<String> {

        private final Queue<String> events =
                new ConcurrentLinkedQueue<>();

        volatile boolean started;
        volatile boolean stopped;

        TestSource(String... events) {
            for (String event : events) {
                this.events.add(event);
            }
        }

        @Override
        public void start() {
            started = true;
        }

        @Override
        public String poll() {
            return events.poll();
        }

        @Override
        public void stop() {
            stopped = true;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    }


    // ---------- Failing Source ----------

    private static class FailingSource implements Source<String> {

        volatile boolean stopped;

        @Override
        public void start() {
        }

        @Override
        public String poll() {
            throw new IllegalStateException("Source polling failed");
        }

        @Override
        public void stop() {
            stopped = true;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    }


    // ---------- Recording Sink ----------

    private static class RecordingSink implements Sink<String> {

        private final CopyOnWriteArrayList<String> received =
                new CopyOnWriteArrayList<>();

        private final CountDownLatch latch;

        volatile boolean started;
        volatile boolean stopped;

        RecordingSink(int expectedEvents) {
            this.latch = new CountDownLatch(expectedEvents);
        }

        @Override
        public void start() {
            started = true;
        }

        @Override
        public void write(String data) {
            received.add(data);
            latch.countDown();
        }

        @Override
        public void stop() {
            stopped = true;
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        boolean awaitEvent() throws InterruptedException {
            return latch.await(1, TimeUnit.SECONDS);
        }
    }


    // ---------- Failing Sink ----------

    private static class FailingSink implements Sink<String> {

        private final AtomicInteger writeAttempts =
                new AtomicInteger();

        @Override
        public void start() {
        }

        @Override
        public void write(String data) {
            writeAttempts.incrementAndGet();
            throw new IllegalStateException("Sink failed");
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }
    }

    // ---------- Retryable Failing Sink ----------

    private static class RetryableFailingSink
            implements Sink<String> {

        private int remainingFailures;
        private volatile int attempts;

        private RetryableFailingSink(
                int failuresBeforeSuccess
        ) {
            this.remainingFailures =
                    failuresBeforeSuccess;
        }

        @Override
        public void start() {
            // No setup required for this test sink.
        }

        @Override
        public void write(String data) {

            attempts++;

            if (remainingFailures > 0) {

                remainingFailures--;

                throw new RuntimeException(
                        "Simulated sink failure"
                );
            }
        }

        @Override
        public void stop() {
            // No cleanup required for this test sink.
        }

        @Override
        public Class<?> getInputType() {
            return String.class;
        }

        public int getAttempts() {
            return attempts;
        }
    }

    // ---------- Single Event Source ----------

    private static class SingleEventSource implements Source<String> {

        private boolean running;
        private boolean emitted;

        @Override
        public void start() {
            running = true;
        }

        @Override
        public String poll() {

            if (!running) {
                throw new IllegalStateException(
                        "Source is not running"
                );
            }

            if (emitted) {
                return null;
            }

            emitted = true;
            return "test-event";
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }
    }

    // ---------- Recording Dead Letter Queue ----------

    private static class RecordingDeadLetterQueue
            implements DeadLetterQueue {

        private final CopyOnWriteArrayList<DeadLetterRecord> records =
                new CopyOnWriteArrayList<>();

        private final CountDownLatch recordLatch =
                new CountDownLatch(1);

        @Override
        public void start() {
        }

        @Override
        public void publish(DeadLetterRecord record) {
            records.add(record);
            recordLatch.countDown();
        }

        @Override
        public void stop() {
        }

        boolean awaitRecord()
                throws InterruptedException {

            return recordLatch.await(
                    2,
                    TimeUnit.SECONDS
            );
        }

        CopyOnWriteArrayList<DeadLetterRecord> getRecords() {
            return records;
        }
    }

    // ---------- Committable Source ----------

    private static class CommittableTestSource
            implements CommittableSource<String> {

        private final Queue<String> events =
                new ConcurrentLinkedQueue<>();

        private final AtomicInteger commitCount =
                new AtomicInteger();

        private boolean running;

        CommittableTestSource(String... events) {
            for (String event : events) {
                this.events.add(event);
            }
        }

        @Override
        public void start() {
            running = true;
        }

        @Override
        public String poll() {
            if (!running) {
                throw new IllegalStateException(
                        "Source is not running"
                );
            }

            return events.poll();
        }

        @Override
        public void commit() {
            commitCount.incrementAndGet();
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public Class<?> getOutputType() {
            return String.class;
        }

        int getCommitCount() {
            return commitCount.get();
        }
    }

    // ---------- Failing Dead Letter Queue ----------

    private static class FailingDeadLetterQueue
            implements DeadLetterQueue {

        @Override
        public void start() {
        }

        @Override
        public void publish(DeadLetterRecord record) {
            throw new RuntimeException(
                    "Simulated DLQ failure"
            );
        }

        @Override
        public void stop() {
        }
    }
}