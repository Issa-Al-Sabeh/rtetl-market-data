package com.issaalsabeh.etl.core;

import org.junit.jupiter.api.Test;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

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
                String::trim;

        Transformer<String, String> upperCaseTransformer =
                String::toUpperCase;

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
                input -> input + "-A";

        Transformer<String, String> second =
                input -> input + "-B";

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

        Transformer<String, String> transformer = input -> {
            if (input.equals("bad")) {
                throw new IllegalArgumentException("Bad event");
            }

            return input.toUpperCase();
        };

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

        assertEquals(1, failingSink.writeAttempts.get());
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
    }
}