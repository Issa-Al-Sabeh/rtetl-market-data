package com.issaalsabeh.etl.monitoring;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.Objects;

public class PipelineMetrics {

    private final MeterRegistry registry;

    private final Counter eventsReceived;
    private final Counter eventsProcessed;
    private final Counter eventsFailed;
    private final Counter eventsRetried;
    private final Counter dlqEvents;

    private final Timer processingLatency;

    public PipelineMetrics(
            MeterRegistry registry,
            String pipelineName) {

        this.registry = Objects.requireNonNull(
                registry,
                "MeterRegistry must not be null"
        );

        if (pipelineName == null || pipelineName.isBlank()) {
            throw new IllegalArgumentException(
                    "Pipeline name must not be null or blank"
            );
        }

        this.eventsReceived = Counter.builder(
                        "pipeline.events.received"
                )
                .description(
                        "Number of events received by the pipeline"
                )
                .tag("pipeline", pipelineName)
                .register(registry);

        this.eventsProcessed = Counter.builder(
                        "pipeline.events.processed"
                )
                .description(
                        "Number of events successfully processed"
                )
                .tag("pipeline", pipelineName)
                .register(registry);

        this.eventsFailed = Counter.builder(
                        "pipeline.events.failed"
                )
                .description(
                        "Number of events that failed processing"
                )
                .tag("pipeline", pipelineName)
                .register(registry);

        this.eventsRetried = Counter.builder(
                        "pipeline.events.retried"
                )
                .description(
                        "Number of events that required at least one retry"
                )
                .tag("pipeline", pipelineName)
                .register(registry);

        this.dlqEvents = Counter.builder(
                        "pipeline.events.dlq"
                )
                .description(
                        "Number of events successfully sent to the dead-letter queue"
                )
                .tag("pipeline", pipelineName)
                .register(registry);

        this.processingLatency = Timer.builder(
                        "pipeline.event.processing.duration"
                )
                .description(
                        "End-to-end processing duration of a pipeline event"
                )
                .tag("pipeline", pipelineName)
                .publishPercentileHistogram()
                .register(registry);
    }

    public void recordReceived() {
        eventsReceived.increment();
    }

    public void recordProcessed() {
        eventsProcessed.increment();
    }

    public void recordFailed() {
        eventsFailed.increment();
    }

    public void recordRetried() {
        eventsRetried.increment();
    }

    public void recordDlq() {
        dlqEvents.increment();
    }

    public Timer.Sample startProcessingTimer() {
        return Timer.start(registry);
    }

    public void recordProcessingLatency(Timer.Sample sample) {
        sample.stop(processingLatency);
    }

    public MeterRegistry getRegistry() {
        return registry;
    }
}