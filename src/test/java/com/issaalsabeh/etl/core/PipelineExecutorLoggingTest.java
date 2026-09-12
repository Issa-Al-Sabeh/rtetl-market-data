package com.issaalsabeh.etl.core;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.issaalsabeh.etl.core.dlq.DeadLetterQueue;
import com.issaalsabeh.etl.core.dlq.DeadLetterRecord;
import com.issaalsabeh.etl.core.retry.RetryPolicy;
import com.issaalsabeh.etl.model.MarketEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineExecutorLoggingTest {

    private Logger logger;
    private ListAppender<ILoggingEvent> listAppender;

    @BeforeEach
    void setUp() {

        logger =
                (Logger) LoggerFactory.getLogger(
                        PipelineExecutor.class
                );

        listAppender = new ListAppender<>();
        listAppender.start();

        logger.addAppender(listAppender);
    }

    @AfterEach
    void tearDown() {

        logger.detachAppender(listAppender);
        listAppender.stop();
    }

    @Test
    void shouldIncludePipelineNameInLogContext() {

        UUID eventId = UUID.randomUUID();

        PipelineExecutor<MarketEvent> executor =
                createFailingExecutor(eventId);

        executor.start();

        ILoggingEvent retryLog =
                findLog("sink_retry");

        assertThat(retryLog.getMDCPropertyMap())
                .containsEntry(
                        "pipeline",
                        "test-pipeline"
                );
    }

    @Test
    void shouldIncludeEventIdInRetryLogContext() {

        UUID eventId = UUID.randomUUID();

        PipelineExecutor<MarketEvent> executor =
                createFailingExecutor(eventId);

        executor.start();

        ILoggingEvent retryLog =
                findLog("sink_retry");

        assertThat(retryLog.getMDCPropertyMap())
                .containsEntry(
                        "eventId",
                        eventId.toString()
                );
    }

    @Test
    void shouldIncludeConnectorNameInRetryLogContext() {

        PipelineExecutor<MarketEvent> executor =
                createFailingExecutor(UUID.randomUUID());

        executor.start();

        ILoggingEvent retryLog =
                findLog("sink_retry");

        assertThat(retryLog.getMDCPropertyMap())
                .containsEntry(
                        "connector",
                        "FailingSink"
                );
    }

    @Test
    void shouldLogRetryAtWarnLevel() {

        PipelineExecutor<MarketEvent> executor =
                createFailingExecutor(UUID.randomUUID());

        executor.start();

        ILoggingEvent retryLog =
                findLog("sink_retry");

        assertThat(retryLog.getLevel())
                .isEqualTo(Level.WARN);
    }

    @Test
    void shouldLogFinalFailureAtErrorLevel() {

        PipelineExecutor<MarketEvent> executor =
                createFailingExecutor(UUID.randomUUID());

        executor.start();

        ILoggingEvent finalFailureLog =
                findLog("sink_write_failed");

        assertThat(finalFailureLog.getLevel())
                .isEqualTo(Level.ERROR);
    }

    private PipelineExecutor<MarketEvent> createFailingExecutor(
            UUID eventId
    ) {

        MarketEvent event =
                new MarketEvent(
                        eventId,
                        "AAPL",
                        new BigDecimal("150.2500"),
                        1000,
                        Instant.now()
                );

        SingleEventSource source =
                new SingleEventSource(event);

        FailingSink sink =
                new FailingSink();

        Pipeline<MarketEvent> pipeline =
                Pipeline.<MarketEvent>builder()
                        .name("test-pipeline")
                        .source(source)
                        .sink(sink)
                        .build();

        RetryPolicy retryPolicy =
                new RetryPolicy(
                        3,
                        0,
                        1.0,
                        0
                );

        return new PipelineExecutor<>(
                pipeline,
                retryPolicy,
                new FailingDeadLetterQueue()
        );
    }

    private ILoggingEvent findLog(String messagePrefix) {

        List<ILoggingEvent> matchingLogs =
                listAppender.list.stream()
                        .filter(
                                event ->
                                        event.getFormattedMessage()
                                                .startsWith(messagePrefix)
                        )
                        .toList();

        assertThat(matchingLogs)
                .as(
                        "Expected log starting with '%s'",
                        messagePrefix
                )
                .isNotEmpty();

        return matchingLogs.get(0);
    }

    private static class SingleEventSource
            implements Source<MarketEvent> {

        private final MarketEvent event;

        private boolean started;
        private boolean emitted;

        private SingleEventSource(MarketEvent event) {
            this.event = event;
        }

        @Override
        public void start() {
            started = true;
        }

        @Override
        public MarketEvent poll() {

            if (!started) {
                throw new IllegalStateException(
                        "Source has not been started"
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
            return MarketEvent.class;
        }
    }

    private static class FailingSink
            implements Sink<MarketEvent> {

        private boolean started;

        @Override
        public void start() {
            started = true;
        }

        @Override
        public void write(MarketEvent data) {

            if (!started) {
                throw new IllegalStateException(
                        "Sink has not been started"
                );
            }

            throw new IllegalStateException(
                    "Simulated sink failure"
            );
        }

        @Override
        public void stop() {
            started = false;
        }

        @Override
        public Class<?> getInputType() {
            return MarketEvent.class;
        }
    }

    private static class FailingDeadLetterQueue
            implements DeadLetterQueue {

        @Override
        public void start() {
        }

        @Override
        public void publish(DeadLetterRecord record) {
            throw new IllegalStateException(
                    "Simulated DLQ failure"
            );
        }

        @Override
        public void stop() {
        }
    }
}