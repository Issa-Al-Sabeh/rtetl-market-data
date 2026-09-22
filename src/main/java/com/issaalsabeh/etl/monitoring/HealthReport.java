package com.issaalsabeh.etl.monitoring;

import java.util.Map;
import java.util.Objects;

public record HealthReport(
        HealthStatus status,
        Map<String, ComponentHealth> components
) {

    public HealthReport {

        Objects.requireNonNull(
                status,
                "Health status must not be null"
        );

        Objects.requireNonNull(
                components,
                "Components map must not be null"
        );

        components = Map.copyOf(components);
    }
}