
package com.issaalsabeh.etl.monitoring;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.issaalsabeh.etl.core.PipelineState;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.time.Duration;

import java.util.List;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricsHttpServerTest {

    private MetricsHttpServer server;

    private PrometheusMeterRegistry registry;

    private HttpClient client;

    private ObjectMapper objectMapper;


    @BeforeEach
    void setUp() {

        registry = new PrometheusMeterRegistry(
                PrometheusConfig.DEFAULT
        );

        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        objectMapper = new ObjectMapper();
    }


    @AfterEach
    void tearDown() {

        if (server != null) {
            server.stop();
        }

        if (registry != null) {
            registry.close();
        }

        if (client != null) {
            client.close();
        }
    }


    @Test
    void shouldServeMetricsAndHealthOnSamePort()
            throws Exception {

        registry.counter(
                "test.requests.total"
        ).increment();

        HealthService healthService =
                new HealthService(
                        () -> PipelineState.RUNNING,
                        List.of()
                );

        server = new MetricsHttpServer(
                registry,
                0,
                healthService
        );

        server.start();

        int port = server.getBoundPort();

        HttpResponse<String> metricsResponse =
                get("/metrics");

        HttpResponse<String> healthResponse =
                get("/health");

        assertThat(metricsResponse.statusCode())
                .isEqualTo(200);

        assertThat(metricsResponse.body())
                .contains("test_requests_total");

        assertThat(healthResponse.statusCode())
                .isEqualTo(200);

        JsonNode healthJson =
                objectMapper.readTree(
                        healthResponse.body()
                );

        assertThat(
                healthJson.path("status").asText()
        ).isEqualTo("HEALTHY");

        assertThat(port)
                .isGreaterThan(0);
    }


    @Test
    void shouldReturnDegradedWhenOptionalDependencyFails()
            throws Exception {

        HealthCheck redis = createHealthCheck(
                "redis",
                false,
                true
        );

        HealthService healthService =
                new HealthService(
                        () -> PipelineState.RUNNING,
                        List.of(redis)
                );

        server = new MetricsHttpServer(
                registry,
                0,
                healthService
        );

        server.start();

        HttpResponse<String> response =
                get("/health");

        assertThat(response.statusCode())
                .isEqualTo(200);

        JsonNode json =
                objectMapper.readTree(
                        response.body()
                );

        assertThat(json.path("status").asText())
                .isEqualTo("DEGRADED");

        assertThat(
                json.path("components")
                        .path("redis")
                        .path("status")
                        .asText()
        ).isEqualTo("UNHEALTHY");
    }


    @Test
    void shouldReturn503WhenCriticalDependencyFails()
            throws Exception {

        HealthCheck kafka = createHealthCheck(
                "kafka",
                true,
                true
        );

        HealthService healthService =
                new HealthService(
                        () -> PipelineState.RUNNING,
                        List.of(kafka)
                );

        server = new MetricsHttpServer(
                registry,
                0,
                healthService
        );

        server.start();

        HttpResponse<String> response =
                get("/health");

        assertThat(response.statusCode())
                .isEqualTo(503);

        JsonNode json =
                objectMapper.readTree(
                        response.body()
                );

        assertThat(json.path("status").asText())
                .isEqualTo("UNHEALTHY");
    }


    @Test
    void shouldPreserveMetricsEndpointWithoutHealthService()
            throws Exception {

        server = new MetricsHttpServer(
                registry,
                0
        );

        server.start();

        HttpResponse<String> response =
                get("/metrics");

        assertThat(response.statusCode())
                .isEqualTo(200);

        assertThat(
                response.headers()
                        .firstValue("Content-Type")
        ).hasValue(
                "text/plain; version=0.0.4; charset=utf-8"
        );
    }


    @Test
    void shouldRejectNonGetHealthRequests()
            throws Exception {

        HealthService healthService =
                new HealthService(
                        () -> PipelineState.RUNNING,
                        List.of()
                );

        server = new MetricsHttpServer(
                registry,
                0,
                healthService
        );

        server.start();

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(
                                URI.create(
                                        "http://127.0.0.1:"
                                                + server.getBoundPort()
                                                + "/health"
                                )
                        )
                        .timeout(Duration.ofSeconds(5))
                        .POST(
                                HttpRequest.BodyPublishers.noBody()
                        )
                        .build();

        HttpResponse<String> response =
                client.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                );

        assertThat(response.statusCode())
                .isEqualTo(405);
    }


    @Test
    void shouldRejectGettingPortBeforeStart() {

        server = new MetricsHttpServer(
                registry,
                0
        );

        assertThatThrownBy(server::getBoundPort)
                .isInstanceOf(
                        IllegalStateException.class
                )
                .hasMessageContaining(
                        "HTTP server is not running"
                );
    }


    @Test
    void shouldServeMetricsWhileHealthCheckIsBlocked()
            throws Exception {

        CountDownLatch healthCheckStarted =
                new CountDownLatch(1);

        CountDownLatch releaseHealthCheck =
                new CountDownLatch(1);

        HealthCheck slowHealthCheck = new HealthCheck() {

            @Override
            public String name() {
                return "slow-dependency";
            }

            @Override
            public boolean isCritical() {
                return false;
            }

            @Override
            public void check() throws Exception {

                healthCheckStarted.countDown();

                if (!releaseHealthCheck.await(
                        5,
                        TimeUnit.SECONDS
                )) {

                    throw new IllegalStateException(
                            "Health check timed out"
                    );
                }
            }
        };

        HealthService healthService =
                new HealthService(
                        () -> PipelineState.RUNNING,
                        List.of(slowHealthCheck)
                );

        registry.counter(
                "test.concurrent.requests"
        ).increment();

        server = new MetricsHttpServer(
                registry,
                0,
                healthService
        );

        server.start();

        CompletableFuture<HttpResponse<String>> healthFuture =
                CompletableFuture.supplyAsync(() -> {

                    try {

                        return get("/health");

                    } catch (Exception e) {

                        throw new RuntimeException(e);
                    }
                });

        try {

            assertThat(
                    healthCheckStarted.await(
                            3,
                            TimeUnit.SECONDS
                    )
            ).isTrue();

            assertThat(healthFuture.isDone())
                    .isFalse();

            HttpResponse<String> metricsResponse =
                    get("/metrics");

            assertThat(metricsResponse.statusCode())
                    .isEqualTo(200);

            assertThat(metricsResponse.body())
                    .contains(
                            "test_concurrent_requests_total"
                    );

            assertThat(healthFuture.isDone())
                    .isFalse();

            releaseHealthCheck.countDown();

            HttpResponse<String> healthResponse =
                    healthFuture.get(
                            3,
                            TimeUnit.SECONDS
                    );

            assertThat(healthResponse.statusCode())
                    .isEqualTo(200);

            JsonNode json =
                    objectMapper.readTree(
                            healthResponse.body()
                    );

            assertThat(json.path("status").asText())
                    .isEqualTo("HEALTHY");

        } finally {

            releaseHealthCheck.countDown();

            healthFuture.cancel(true);
        }
    }


    @Test
    void shouldStopServerAndReleaseResources()
            throws Exception {

        HealthService healthService =
                new HealthService(
                        () -> PipelineState.RUNNING,
                        List.of()
                );

        server = new MetricsHttpServer(
                registry,
                0,
                healthService
        );

        server.start();

        HttpResponse<String> response =
                get("/health");

        assertThat(response.statusCode())
                .isEqualTo(200);

        server.stop();

        assertThatThrownBy(server::getBoundPort)
                .isInstanceOf(
                        IllegalStateException.class
                );

        server.stop();
    }


    @Test
    void shouldHandleMultipleConcurrentHealthRequests()
            throws Exception {

        AtomicInteger checksExecuted =
                new AtomicInteger();

        HealthCheck healthCheck = new HealthCheck() {

            @Override
            public String name() {
                return "test-dependency";
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

        HealthService healthService =
                new HealthService(
                        () -> PipelineState.RUNNING,
                        List.of(healthCheck)
                );

        server = new MetricsHttpServer(
                registry,
                0,
                healthService
        );

        server.start();

        int numberOfRequests = 5;

        for (int i = 0; i < numberOfRequests; i++) {

            HttpResponse<String> response =
                    get("/health");

            assertThat(response.statusCode())
                    .isEqualTo(200);
        }

        assertThat(checksExecuted.get())
                .isEqualTo(numberOfRequests);
    }


    private HttpResponse<String> get(
            String path
    ) throws Exception {

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(
                                URI.create(
                                        "http://127.0.0.1:"
                                                + server.getBoundPort()
                                                + path
                                )
                        )
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build();

        return client.send(
                request,
                HttpResponse.BodyHandlers.ofString()
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
                            name + " unavailable"
                    );
                }
            }
        };
    }
}