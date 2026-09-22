
package com.issaalsabeh.etl.monitoring;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import com.issaalsabeh.etl.core.PipelineState;
import java.util.function.Supplier;

public class HealthService {

    private final Supplier<PipelineState> applicationRunning;
    private final List<HealthCheck> healthChecks;

    public HealthService(
            Supplier<PipelineState> applicationRunning,
            List<HealthCheck> healthChecks
    ) {

        this.applicationRunning = Objects.requireNonNull(
                applicationRunning,
                "Application state supplier must not be null"
        );

        this.healthChecks = List.copyOf(
                Objects.requireNonNull(
                        healthChecks,
                        "Health checks must not be null"
                )
        );
    }

    public HealthReport checkHealth() {

        Map<String, ComponentHealth> components =
                new LinkedHashMap<>();

        boolean isRunning =
                applicationRunning.get() == PipelineState.RUNNING;

        components.put(
                "application",
                new ComponentHealth(
                        isRunning
                                ? HealthStatus.HEALTHY
                                : HealthStatus.UNHEALTHY
                )
        );

        HealthStatus overallStatus =
                isRunning
                        ? HealthStatus.HEALTHY
                        : HealthStatus.UNHEALTHY;

        for (HealthCheck healthCheck : healthChecks) {

            try {

                healthCheck.check();

                components.put(
                        healthCheck.name(),
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        )
                );

            } catch (Exception e) {

                components.put(
                        healthCheck.name(),
                        new ComponentHealth(
                                HealthStatus.UNHEALTHY
                        )
                );

                if (healthCheck.isCritical()) {

                    overallStatus = HealthStatus.UNHEALTHY;

                } else if (
                        overallStatus != HealthStatus.UNHEALTHY
                ) {

                    overallStatus = HealthStatus.DEGRADED;
                }
            }
        }

        return new HealthReport(
                overallStatus,
                components
        );
    }
}