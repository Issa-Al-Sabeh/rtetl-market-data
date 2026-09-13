package com.issaalsabeh.etl.core;

import com.issaalsabeh.etl.core.dlq.DeadLetterQueue;
import com.issaalsabeh.etl.core.dlq.DeadLetterRecord;
import com.issaalsabeh.etl.core.dlq.NoOpDeadLetterQueue;
import com.issaalsabeh.etl.core.retry.RetryPolicy;
import com.issaalsabeh.etl.monitoring.PipelineMetrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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

    private final PipelineMetrics metrics;


    public PipelineExecutor(Pipeline<T> pipeline) {
        this(
                pipeline,
                RetryPolicy.defaultPolicy(),
                new NoOpDeadLetterQueue(),
                createDefaultMetrics(pipeline)
        );
    }

    public PipelineExecutor(
            Pipeline<T> pipeline,
            RetryPolicy retryPolicy
    ) {
        this(
                pipeline,
                retryPolicy,
                new NoOpDeadLetterQueue(),
                createDefaultMetrics(pipeline)
        );
    }

    public PipelineExecutor(
            Pipeline<T> pipeline,
            RetryPolicy retryPolicy,
            DeadLetterQueue deadLetterQueue
    ) {
        this(
                pipeline,
                retryPolicy,
                deadLetterQueue,
                createDefaultMetrics(pipeline)
        );
    }

    public PipelineExecutor(
            Pipeline<T> pipeline,
            RetryPolicy retryPolicy,
            DeadLetterQueue deadLetterQueue,
            PipelineMetrics metrics) {

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

        if (metrics == null) {
            throw new IllegalArgumentException(
                    "Pipeline Metrics cannot be null"
            );
        }

        this.pipeline = pipeline;
        this.retryPolicy = retryPolicy;
        this.deadLetterQueue = deadLetterQueue;
        this.shutdownRequested = false;
        this.metrics = metrics;
    }

    private static PipelineMetrics createDefaultMetrics(
            Pipeline<?> pipeline
    ) {

        if (pipeline == null) {
            throw new IllegalArgumentException(
                    "Pipeline cannot be null"
            );
        }

        return new PipelineMetrics(
                new SimpleMeterRegistry(),
                pipeline.getName()
        );
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

                if (pipeline.getSource() instanceof MeterBinder meterBinder) {
                    meterBinder
                            .bindTo(metrics.getRegistry());
                }

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

                metrics.recordReceived();

                Timer.Sample processingSample =
                        metrics.startProcessingTimer();

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

                        metrics.recordFailed();

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
                    boolean eventRetried = false;
                    boolean eventFailed = false;

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

                            SinkWriteResult result =
                                    writeWithRetry(
                                            typedSink,
                                            current
                                    );

                            if (result.retryCount() > 0) {
                                eventRetried = true;
                            }

                            if (result.outcome() != SinkWriteOutcome.SUCCESS) {
                                eventFailed = true;
                            }

                            if (result.outcome() == SinkWriteOutcome.UNRESOLVED) {
                                eventHandled = false;
                            }

                        } catch (Exception e) {

                            eventHandled = false;
                            eventFailed = true;

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

                    if (eventRetried) {
                        metrics.recordRetried();
                    }

                    if (eventFailed) {
                        metrics.recordFailed();
                    } else {
                        metrics.recordProcessed();
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

                    metrics.recordProcessingLatency(processingSample);

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

    private SinkWriteResult writeWithRetry(
            Sink<Object> sink,
            Object event
    ) {

        for (int attempt = 1;
             attempt <= retryPolicy.maxAttempts();
             attempt++) {

            try {

                sink.write(event);

                return new SinkWriteResult(
                        SinkWriteOutcome.SUCCESS,
                        attempt - 1
                );

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

                        metrics.recordDlq();

                        logger.warn(
                                "event_dead_lettered failedSink={} retryCount={}",
                                sink.getClass().getSimpleName(),
                                attempt - 1
                        );

                        return new SinkWriteResult(
                                SinkWriteOutcome.DEAD_LETTERED,
                                attempt - 1
                        );

                    } catch (Exception dlqException) {

                        logger.error(
                                "dead_letter_publish_failed failedSink={} errorType={} errorMessage={}",
                                sink.getClass().getSimpleName(),
                                dlqException.getClass().getName(),
                                dlqException.getMessage(),
                                dlqException
                        );

                        return new SinkWriteResult(
                                SinkWriteOutcome.UNRESOLVED,
                                attempt - 1
                        );

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

                    return new SinkWriteResult(
                            SinkWriteOutcome.UNRESOLVED,
                            attempt - 1
                    );
                }
            }
        }

        return new SinkWriteResult(
                SinkWriteOutcome.UNRESOLVED,
                retryPolicy.maxAttempts() - 1
        );
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

    private enum SinkWriteOutcome {
        SUCCESS,
        DEAD_LETTERED,
        UNRESOLVED
    }

    private record SinkWriteResult(
            SinkWriteOutcome outcome,
            int retryCount
    ) {
    }
}