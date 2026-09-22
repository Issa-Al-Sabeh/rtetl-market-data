
package com.issaalsabeh.etl.monitoring;

import com.issaalsabeh.etl.core.PipelineState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class HealthServiceTest {

    @Test
    void shouldReturnHealthyWhenAllChecksPass() {

        HealthCheck kafka = createHealthCheck(
                "kafka",
                true,
                false
        );

        HealthCheck postgres = createHealthCheck(
                "postgres",
                true,
                false
        );

        HealthCheck redis = createHealthCheck(
                "redis",
                false,
                false
        );

        HealthService healthService = new HealthService(
                () -> PipelineState.RUNNING,
                List.of(kafka, postgres, redis)
        );

        HealthReport report = healthService.checkHealth();

        assertThat(report.status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components())
                .hasSize(4);

        assertThat(report.components().get("application").status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components().get("kafka").status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components().get("postgres").status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components().get("redis").status())
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    void shouldReturnDegradedWhenOptionalDependencyFails() {

        HealthCheck kafka = createHealthCheck(
                "kafka",
                true,
                false
        );

        HealthCheck postgres = createHealthCheck(
                "postgres",
                true,
                false
        );

        HealthCheck redis = createHealthCheck(
                "redis",
                false,
                true
        );

        HealthService healthService = new HealthService(
                () -> PipelineState.RUNNING,
                List.of(kafka, postgres, redis)
        );

        HealthReport report = healthService.checkHealth();

        assertThat(report.status())
                .isEqualTo(HealthStatus.DEGRADED);

        assertThat(report.components().get("application").status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components().get("kafka").status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components().get("postgres").status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components().get("redis").status())
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    void shouldReturnUnhealthyWhenCriticalDependencyFails() {

        HealthCheck kafka = createHealthCheck(
                "kafka",
                true,
                true
        );

        HealthCheck postgres = createHealthCheck(
                "postgres",
                true,
                false
        );

        HealthCheck redis = createHealthCheck(
                "redis",
                false,
                false
        );

        HealthService healthService = new HealthService(
                () -> PipelineState.RUNNING,
                List.of(kafka, postgres, redis)
        );

        HealthReport report = healthService.checkHealth();

        assertThat(report.status())
                .isEqualTo(HealthStatus.UNHEALTHY);

        assertThat(report.components().get("application").status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components().get("kafka").status())
                .isEqualTo(HealthStatus.UNHEALTHY);

        assertThat(report.components().get("postgres").status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components().get("redis").status())
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    void shouldReturnUnhealthyWhenApplicationIsNotRunning() {

        HealthCheck kafka = createHealthCheck(
                "kafka",
                true,
                false
        );

        HealthCheck redis = createHealthCheck(
                "redis",
                false,
                false
        );

        HealthService healthService = new HealthService(
                () -> PipelineState.STOPPED,
                List.of(kafka, redis)
        );

        HealthReport report = healthService.checkHealth();

        assertThat(report.status())
                .isEqualTo(HealthStatus.UNHEALTHY);

        assertThat(report.components().get("application").status())
                .isEqualTo(HealthStatus.UNHEALTHY);

        assertThat(report.components().get("kafka").status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components().get("redis").status())
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    void shouldContinueCheckingAfterOneDependencyFails() {

        AtomicInteger checksExecuted = new AtomicInteger(0);

        HealthCheck kafka = new HealthCheck() {

            @Override
            public String name() {
                return "kafka";
            }

            @Override
            public boolean isCritical() {
                return true;
            }

            @Override
            public void check() throws Exception {

                checksExecuted.incrementAndGet();

                throw new Exception("Kafka unavailable");
            }
        };

        HealthCheck postgres = new HealthCheck() {

            @Override
            public String name() {
                return "postgres";
            }

            @Override
            public boolean isCritical() {
                return true;
            }

            @Override
            public void check() {

                checksExecuted.incrementAndGet();
            }
        };

        HealthCheck redis = new HealthCheck() {

            @Override
            public String name() {
                return "redis";
            }

            @Override
            public boolean isCritical() {
                return false;
            }

            @Override
            public void check() {

                checksExecuted.incrementAndGet();
            }
        };

        HealthService healthService = new HealthService(
                () -> PipelineState.RUNNING,
                List.of(kafka, postgres, redis)
        );

        HealthReport report = healthService.checkHealth();

        assertThat(checksExecuted.get())
                .isEqualTo(3);

        assertThat(report.components())
                .containsKeys(
                        "application",
                        "kafka",
                        "postgres",
                        "redis"
                );

        assertThat(report.components().get("kafka").status())
                .isEqualTo(HealthStatus.UNHEALTHY);

        assertThat(report.components().get("postgres").status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components().get("redis").status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.status())
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    void shouldOmitDependenciesThatAreNotConfigured() {

        HealthCheck kafka = createHealthCheck(
                "kafka",
                true,
                false
        );

        HealthService healthService = new HealthService(
                () -> PipelineState.RUNNING,
                List.of(kafka)
        );

        HealthReport report = healthService.checkHealth();

        assertThat(report.status())
                .isEqualTo(HealthStatus.HEALTHY);

        assertThat(report.components())
                .hasSize(2)
                .containsOnlyKeys(
                        "application",
                        "kafka"
                );

        assertThat(report.components())
                .doesNotContainKeys(
                        "postgres",
                        "redis"
                );
    }

    private HealthCheck createHealthCheck(
            String name,
            boolean critical,
            boolean shouldFail
    ) {

        return new HealthCheck() {

            @Override
            public String name() {
                return name;
            }

            @Override
            public boolean isCritical() {
                return critical;
            }

            @Override
            public void check() throws Exception {

                if (shouldFail) {
                    throw new Exception(
                            name + " is unavailable"
                    );
                }
            }
        };
    }
}