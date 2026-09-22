package com.issaalsabeh.etl.monitoring;

import java.util.Objects;

public record ComponentHealth(
        HealthStatus status
) {

    public ComponentHealth {
        Objects.requireNonNull(
                status,
                "Health status must not be null"
        );
    }
}