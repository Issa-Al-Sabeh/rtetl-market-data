package com.issaalsabeh.etl.monitoring;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineMetricsTest {

    private SimpleMeterRegistry registry;
    private PipelineMetrics metrics;

    @BeforeEach
    void setUp() {

        registry = new SimpleMeterRegistry();

        metrics = new PipelineMetrics(
                registry,
                "test-pipeline"
        );
    }

    @Test
    void shouldRecordReceivedEvent() {

        metrics.recordReceived();

        assertThat(
                registry.get("pipeline.events.received")
                        .tag("pipeline", "test-pipeline")
                        .counter()
                        .count()
        ).isEqualTo(1.0);
    }

    @Test
    void shouldRecordProcessedEvent() {

        metrics.recordProcessed();

        assertThat(
                registry.get("pipeline.events.processed")
                        .tag("pipeline", "test-pipeline")
                        .counter()
                        .count()
        ).isEqualTo(1.0);
    }

    @Test
    void shouldRecordFailedEvent() {

        metrics.recordFailed();

        assertThat(
                registry.get("pipeline.events.failed")
                        .tag("pipeline", "test-pipeline")
                        .counter()
                        .count()
        ).isEqualTo(1.0);
    }

    @Test
    void shouldRecordRetriedEvent() {

        metrics.recordRetried();

        assertThat(
                registry.get("pipeline.events.retried")
                        .tag("pipeline", "test-pipeline")
                        .counter()
                        .count()
        ).isEqualTo(1.0);
    }

    @Test
    void shouldRecordDlqEvent() {

        metrics.recordDlq();

        assertThat(
                registry.get("pipeline.events.dlq")
                        .tag("pipeline", "test-pipeline")
                        .counter()
                        .count()
        ).isEqualTo(1.0);
    }

    @Test
    void shouldRecordProcessingLatency() {

        var sample = metrics.startProcessingTimer();

        metrics.recordProcessingLatency(sample);

        var timer = registry.get(
                        "pipeline.event.processing.duration"
                )
                .tag("pipeline", "test-pipeline")
                .timer();

        assertThat(timer.count()).isEqualTo(1);

        assertThat(
                timer.totalTime(TimeUnit.NANOSECONDS)
        ).isGreaterThanOrEqualTo(0);
    }
}