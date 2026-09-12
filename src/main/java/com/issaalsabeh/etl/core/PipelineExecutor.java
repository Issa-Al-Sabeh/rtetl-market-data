package com.issaalsabeh.etl.core;

import com.issaalsabeh.etl.core.dlq.DeadLetterQueue;
import com.issaalsabeh.etl.core.dlq.DeadLetterRecord;
import com.issaalsabeh.etl.core.dlq.NoOpDeadLetterQueue;
import com.issaalsabeh.etl.core.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;

public class PipelineExecutor<T> {

    private static final Logger logger =
            LoggerFactory.getLogger(PipelineExecutor.class);

    private final Pipeline<T> pipeline;

    private volatile boolean shutdownRequested;

    private final RetryPolicy retryPolicy;

    private final DeadLetterQueue deadLetterQueue;

    private final CountDownLatch terminated = new CountDownLatch(1);

    public PipelineExecutor(Pipeline<T> pipeline) {
        this(
                pipeline,
                RetryPolicy.defaultPolicy(),
                new NoOpDeadLetterQueue()
        );
    }

    public PipelineExecutor(
            Pipeline<T> pipeline,
            RetryPolicy retryPolicy
    ) {
        this(
                pipeline,
                retryPolicy,
                new NoOpDeadLetterQueue()
        );
    }

    public PipelineExecutor(
            Pipeline<T> pipeline,
            RetryPolicy retryPolicy,
            DeadLetterQueue deadLetterQueue
    ) {
        if (pipeline == null) {
            throw new IllegalArgumentException(
                    "Pipeline cannot be null"
            );
        }

        if (retryPolicy == null) {
            throw new IllegalArgumentException(
                    "Retry policy cannot be null"
            );
        }

        if (deadLetterQueue == null) {
            throw new IllegalArgumentException(
                    "Dead letter queue cannot be null"
            );
        }

        this.pipeline = pipeline;
        this.retryPolicy = retryPolicy;
        this.deadLetterQueue = deadLetterQueue;
        this.shutdownRequested = false;
    }

    public void start() {

        pipeline.validate();

        try {

            pipeline.getSource().start();

            deadLetterQueue.start();

            for (Sink<?> sink : pipeline.getSinks()) {
                sink.start();
            }


            while (!shutdownRequested) {

                T event = pipeline.getSource().poll();

                if (event == null) {
                    continue;
                }

                Object current = event;

                if (shutdownRequested) {
                    break;
                }

                try {

                    for (Transformer<?, ?> transformer
                            : pipeline.getTransformers()) {

                        @SuppressWarnings("unchecked")
                        Transformer<Object, Object> typedTransformer =
                                (Transformer<Object, Object>) transformer;

                        current =
                                typedTransformer.transform(current);
                    }

                } catch (Exception e) {

                    logger.error(
                            "Failed to transform event: {}",
                            current,
                            e
                    );

                    if (pipeline.getSource() instanceof CommittableSource<?> source) {
                        source.commit();
                    }

                    continue;
                }

                boolean eventHandled = true;

                for (Sink<?> sink : pipeline.getSinks()) {

                    try {

                        @SuppressWarnings("unchecked")
                        Sink<Object> typedSink =
                                (Sink<Object>) sink;

                        boolean sinkHandled =
                                writeWithRetry(
                                        typedSink,
                                        current
                                );

                        if (!sinkHandled) {
                            eventHandled = false;
                        }

                    } catch (Exception e) {

                        eventHandled = false;

                        logger.error(
                                "Failed to handle event {} for sink {}",
                                current,
                                sink.getClass().getSimpleName(),
                                e
                        );
                    }
                }

                if (!eventHandled) {

                    logger.error(
                            "Event could not be fully handled. Stopping pipeline to avoid committing past an unresolved offset."
                    );

                    break;
                }

                if (pipeline.getSource() instanceof CommittableSource<?> source) {
                    source.commit();
                }
            }

        } finally {

            shutdownRequested = false;

            cleanupResources();

            terminated.countDown();
        }
    }

    public void stop(){
        shutdownRequested = true;
    }

    private boolean writeWithRetry(
            Sink<Object> sink,
            Object event
    ) {
        for (int attempt = 1;
             attempt <= retryPolicy.maxAttempts();
             attempt++) {

            try {
                sink.write(event);
                return true;

            } catch (Exception e) {

                if (attempt == retryPolicy.maxAttempts()) {

                    logger.error(
                            "Sink {} failed after {} attempts",
                            sink.getClass().getSimpleName(),
                            retryPolicy.maxAttempts(),
                            e
                    );

                    DeadLetterRecord record =
                            new DeadLetterRecord(
                                    event,
                                    sink.getClass().getSimpleName(),
                                    e.getClass().getName(),
                                    e.getMessage(),
                                    Instant.now(),
                                    attempt - 1
                            );

                    try {
                        deadLetterQueue.publish(record);

                        logger.info(
                                "Event sent to dead-letter queue after sink {} failed",
                                sink.getClass().getSimpleName()
                        );

                        return true;

                    } catch (Exception dlqException) {

                        logger.error(
                                "Failed to publish event to dead-letter queue after sink {} exhausted retries",
                                sink.getClass().getSimpleName(),
                                dlqException
                        );

                        return false;
                    }
                }

                long delay =
                        retryPolicy.getDelayMillis(attempt);

                logger.warn(
                        "Sink {} failed on attempt {}/{}. Retrying in {} ms",
                        sink.getClass().getSimpleName(),
                        attempt,
                        retryPolicy.maxAttempts(),
                        delay,
                        e
                );

                try {
                    Thread.sleep(delay);

                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }

        return false;
    }

    public void awaitTermination() throws InterruptedException {
        terminated.await();
    }

    private void cleanupResources() {

        for (Sink<?> sink : pipeline.getSinks()) {
            try {
                sink.stop();
            } catch (Exception e) {
                logger.error(
                        "Failed to stop sink {}",
                        sink.getClass().getSimpleName(),
                        e
                );
            }
        }

        try {
            deadLetterQueue.stop();
        } catch (Exception e) {
            logger.error(
                    "Failed to stop Dead Letter Queue",
                    e
            );
        }

        try {
            pipeline.getSource().stop();
        } catch (Exception e) {
            logger.error(
                    "Failed to stop Source",
                    e
            );
        }
    }
}