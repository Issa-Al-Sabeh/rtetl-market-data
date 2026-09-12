package com.issaalsabeh.etl.core;

import com.issaalsabeh.etl.core.dlq.DeadLetterQueue;
import com.issaalsabeh.etl.core.dlq.DeadLetterRecord;
import com.issaalsabeh.etl.core.dlq.NoOpDeadLetterQueue;
import com.issaalsabeh.etl.core.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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

        MDC.put("pipeline", pipeline.getName());

        try {

            pipeline.validate();

            MDC.put(
                    "connector",
                    pipeline.getSource().getClass().getSimpleName()
            );

            try {
                pipeline.getSource().start();
            } finally {
                MDC.remove("connector");
            }

            MDC.put(
                    "connector",
                    deadLetterQueue.getClass().getSimpleName()
            );

            try {
                deadLetterQueue.start();
            } finally {
                MDC.remove("connector");
            }

            for (Sink<?> sink : pipeline.getSinks()) {

                MDC.put(
                        "connector",
                        sink.getClass().getSimpleName()
                );

                try {
                    sink.start();
                } finally {
                    MDC.remove("connector");
                }
            }

            while (!shutdownRequested) {

                T event;

                MDC.put(
                        "connector",
                        pipeline.getSource().getClass().getSimpleName()
                );

                try {
                    event = pipeline.getSource().poll();
                } finally {
                    MDC.remove("connector");
                }

                if (event == null) {
                    continue;
                }

                if (shutdownRequested) {
                    break;
                }

                String eventId = getEventId(event);

                if (eventId != null) {
                    MDC.put("eventId", eventId);
                } else {
                    MDC.remove("eventId");
                }

                try {

                    Object current = event;

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

                        logger.warn(
                                "event_transformation_failed errorType={} errorMessage={}",
                                e.getClass().getSimpleName(),
                                e.getMessage()
                        );

                        if (pipeline.getSource()
                                instanceof CommittableSource<?> source) {

                            MDC.put(
                                    "connector",
                                    source.getClass().getSimpleName()
                            );

                            try {
                                source.commit();
                            } finally {
                                MDC.remove("connector");
                            }
                        }

                        continue;
                    }

                    boolean eventHandled = true;

                    for (Sink<?> sink : pipeline.getSinks()) {

                        String connectorName =
                                sink.getClass().getSimpleName();

                        MDC.put(
                                "connector",
                                connectorName
                        );

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
                                    "sink_handling_failed errorType={} errorMessage={}",
                                    e.getClass().getName(),
                                    e.getMessage(),
                                    e
                            );

                        } finally {

                            MDC.remove("connector");
                        }
                    }

                    if (!eventHandled) {

                        logger.error(
                                "event_unresolved stoppingPipeline=true reason=no_sink_or_dlq_acceptance"
                        );

                        break;
                    }

                    if (pipeline.getSource()
                            instanceof CommittableSource<?> source) {

                        MDC.put(
                                "connector",
                                source.getClass().getSimpleName()
                        );

                        try {
                            source.commit();
                        } finally {
                            MDC.remove("connector");
                        }
                    }

                } finally {

                    MDC.remove("eventId");
                    MDC.remove("connector");
                }
            }

        } finally {

            shutdownRequested = false;

            try {

                cleanupResources();

            } finally {

                terminated.countDown();

                MDC.remove("connector");
                MDC.remove("eventId");
                MDC.remove("pipeline");
            }
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
                            "sink_write_failed attempts={} errorType={} errorMessage={}",
                            attempt,
                            e.getClass().getName(),
                            e.getMessage(),
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

                    String previousConnector =
                            MDC.get("connector");

                    MDC.put(
                            "connector",
                            deadLetterQueue
                                    .getClass()
                                    .getSimpleName()
                    );

                    try {

                        deadLetterQueue.publish(record);

                        logger.warn(
                                "event_dead_lettered failedSink={} retryCount={}",
                                sink.getClass().getSimpleName(),
                                attempt - 1
                        );

                        return true;

                    } catch (Exception dlqException) {

                        logger.error(
                                "dead_letter_publish_failed failedSink={} errorType={} errorMessage={}",
                                sink.getClass().getSimpleName(),
                                dlqException.getClass().getName(),
                                dlqException.getMessage(),
                                dlqException
                        );

                        return false;

                    } finally {

                        if (previousConnector != null) {
                            MDC.put(
                                    "connector",
                                    previousConnector
                            );
                        } else {
                            MDC.remove("connector");
                        }
                    }
                }

                long delay =
                        retryPolicy.getDelayMillis(attempt);

                logger.warn(
                        "sink_retry attempt={} maxAttempts={} delayMs={} errorType={} errorMessage={}",
                        attempt,
                        retryPolicy.maxAttempts(),
                        delay,
                        e.getClass().getSimpleName(),
                        e.getMessage()
                );

                try {

                    Thread.sleep(delay);

                } catch (InterruptedException interruptedException) {

                    Thread.currentThread().interrupt();

                    logger.warn(
                            "sink_retry_interrupted attempt={} maxAttempts={}",
                            attempt,
                            retryPolicy.maxAttempts()
                    );

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

            MDC.put(
                    "connector",
                    sink.getClass().getSimpleName()
            );

            try {

                sink.stop();

            } catch (Exception e) {

                logger.error(
                        "connector_stop_failed errorType={} errorMessage={}",
                        e.getClass().getName(),
                        e.getMessage(),
                        e
                );

            } finally {

                MDC.remove("connector");
            }
        }

        MDC.put(
                "connector",
                deadLetterQueue.getClass().getSimpleName()
        );

        try {

            deadLetterQueue.stop();

        } catch (Exception e) {

            logger.error(
                    "connector_stop_failed errorType={} errorMessage={}",
                    e.getClass().getName(),
                    e.getMessage(),
                    e
            );

        } finally {

            MDC.remove("connector");
        }

        MDC.put(
                "connector",
                pipeline.getSource().getClass().getSimpleName()
        );

        try {

            pipeline.getSource().stop();

        } catch (Exception e) {

            logger.error(
                    "connector_stop_failed errorType={} errorMessage={}",
                    e.getClass().getName(),
                    e.getMessage(),
                    e
            );

        } finally {

            MDC.remove("connector");
        }
    }

    private String getEventId(Object event) {

        if (event instanceof IdentifiableEvent identifiable
                && identifiable.eventId() != null) {

            return identifiable.eventId().toString();
        }

        return null;
    }
}