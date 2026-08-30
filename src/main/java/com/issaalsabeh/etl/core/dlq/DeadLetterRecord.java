package com.issaalsabeh.etl.core.dlq;

import java.time.Instant;

public record DeadLetterRecord(
        Object originalEvent,
        String failedSink,
        String errorType,
        String errorMessage,
        Instant failureTimestamp,
        int retryCount
) {
}