
package com.issaalsabeh.etl.monitoring;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Mockito.*;


class HealthHttpHandlerTest {

    private HealthService healthService;

    private HealthHttpHandler handler;

    private HttpExchange exchange;

    private ByteArrayOutputStream responseBody;

    private Headers responseHeaders;

    private ObjectMapper objectMapper;


    // =========================================================
    // Test Setup
    // =========================================================

    @BeforeEach
    void setUp() {

        healthService = mock(HealthService.class);

        handler = new HealthHttpHandler(
                healthService
        );

        exchange = mock(HttpExchange.class);

        responseBody = new ByteArrayOutputStream();

        responseHeaders = new Headers();

        objectMapper = new ObjectMapper();


        when(exchange.getRequestMethod())
                .thenReturn("GET");

        when(exchange.getResponseHeaders())
                .thenReturn(responseHeaders);

        when(exchange.getResponseBody())
                .thenReturn(responseBody);
    }


    // =========================================================
    // Test 1: Healthy Application
    // =========================================================

    @Test
    void shouldReturn200WhenApplicationIsHealthy()
            throws IOException {

        HealthReport report = new HealthReport(
                HealthStatus.HEALTHY,
                Map.of(
                        "application",
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        ),
                        "kafka",
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        ),
                        "postgres",
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        ),
                        "redis",
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        )
                )
        );

        when(healthService.checkHealth())
                .thenReturn(report);


        // Execute HTTP handler.

        handler.handle(exchange);


        // Verify HTTP status code.

        verify(exchange).sendResponseHeaders(
                200,
                responseBody.size()
        );


        // Verify JSON response.

        JsonNode json = getResponseJson();

        assertThat(json.path("status").asText())
                .isEqualTo("HEALTHY");

        assertThat(
                json.path("components")
                        .path("application")
                        .path("status")
                        .asText()
        ).isEqualTo("HEALTHY");

        assertThat(
                json.path("components")
                        .path("kafka")
                        .path("status")
                        .asText()
        ).isEqualTo("HEALTHY");

        assertThat(
                json.path("components")
                        .path("postgres")
                        .path("status")
                        .asText()
        ).isEqualTo("HEALTHY");

        assertThat(
                json.path("components")
                        .path("redis")
                        .path("status")
                        .asText()
        ).isEqualTo("HEALTHY");


        // Verify that the health service was called.

        verify(healthService).checkHealth();
    }


    // =========================================================
    // Test 2: Degraded Application
    // =========================================================

    @Test
    void shouldReturn200WhenApplicationIsDegraded()
            throws IOException {

        HealthReport report = new HealthReport(
                HealthStatus.DEGRADED,
                Map.of(
                        "application",
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        ),
                        "kafka",
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        ),
                        "postgres",
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        ),
                        "redis",
                        new ComponentHealth(
                                HealthStatus.UNHEALTHY
                        )
                )
        );

        when(healthService.checkHealth())
                .thenReturn(report);


        handler.handle(exchange);


        // A degraded application must return HTTP 200.

        verify(exchange).sendResponseHeaders(
                200,
                responseBody.size()
        );


        JsonNode json = getResponseJson();

        assertThat(json.path("status").asText())
                .isEqualTo("DEGRADED");


        // Redis is unavailable.

        assertThat(
                json.path("components")
                        .path("redis")
                        .path("status")
                        .asText()
        ).isEqualTo("UNHEALTHY");


        // Critical dependencies remain healthy.

        assertThat(
                json.path("components")
                        .path("kafka")
                        .path("status")
                        .asText()
        ).isEqualTo("HEALTHY");

        assertThat(
                json.path("components")
                        .path("postgres")
                        .path("status")
                        .asText()
        ).isEqualTo("HEALTHY");


        verify(healthService).checkHealth();
    }


    // =========================================================
    // Test 3: Unhealthy Application
    // =========================================================

    @Test
    void shouldReturn503WhenApplicationIsUnhealthy()
            throws IOException {

        HealthReport report = new HealthReport(
                HealthStatus.UNHEALTHY,
                Map.of(
                        "application",
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        ),
                        "kafka",
                        new ComponentHealth(
                                HealthStatus.UNHEALTHY
                        ),
                        "postgres",
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        ),
                        "redis",
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        )
                )
        );

        when(healthService.checkHealth())
                .thenReturn(report);


        handler.handle(exchange);


        // An unhealthy application must return HTTP 503.

        verify(exchange).sendResponseHeaders(
                503,
                responseBody.size()
        );


        JsonNode json = getResponseJson();

        assertThat(json.path("status").asText())
                .isEqualTo("UNHEALTHY");

        assertThat(
                json.path("components")
                        .path("kafka")
                        .path("status")
                        .asText()
        ).isEqualTo("UNHEALTHY");


        verify(healthService).checkHealth();
    }


    // =========================================================
    // Test 4: Reject Unsupported HTTP Methods
    // =========================================================

    @Test
    void shouldRejectNonGetRequests()
            throws IOException {

        when(exchange.getRequestMethod())
                .thenReturn("POST");


        handler.handle(exchange);


        // Verify HTTP 405 Method Not Allowed.

        verify(exchange).sendResponseHeaders(
                405,
                -1
        );


        // Verify the allowed HTTP method.

        assertThat(
                responseHeaders.getFirst("Allow")
        ).isEqualTo("GET");


        // Unsupported requests must not execute health checks.

        verifyNoInteractions(healthService);


        // Verify that no response body was written.

        assertThat(responseBody.size())
                .isZero();


        verify(exchange).close();
    }


    // =========================================================
    // Test 5: Unexpected Health Service Failure
    // =========================================================

    @Test
    void shouldReturn503WhenHealthServiceThrows()
            throws IOException {

        when(healthService.checkHealth())
                .thenThrow(
                        new IllegalStateException(
                                "Internal database password: secret123"
                        )
                );


        handler.handle(exchange);


        // Unexpected internal failures must return HTTP 503.

        verify(exchange).sendResponseHeaders(
                503,
                responseBody.size()
        );


        JsonNode json = getResponseJson();

        assertThat(json.path("status").asText())
                .isEqualTo("UNHEALTHY");


        // The application component must be unhealthy.

        assertThat(
                json.path("components")
                        .path("application")
                        .path("status")
                        .asText()
        ).isEqualTo("UNHEALTHY");


        // Internal exception details must not be exposed.

        String response = responseBody.toString(
                StandardCharsets.UTF_8
        );

        assertThat(response)
                .doesNotContain(
                        "secret123",
                        "Internal database password",
                        "IllegalStateException"
                );


        verify(healthService).checkHealth();
    }


    // =========================================================
    // Test 6: HTTP Response Headers
    // =========================================================

    @Test
    void shouldReturnCorrectResponseHeaders()
            throws IOException {

        HealthReport report = new HealthReport(
                HealthStatus.HEALTHY,
                Map.of(
                        "application",
                        new ComponentHealth(
                                HealthStatus.HEALTHY
                        )
                )
        );

        when(healthService.checkHealth())
                .thenReturn(report);


        handler.handle(exchange);


        // Verify JSON content type.

        assertThat(
                responseHeaders.getFirst("Content-Type")
        ).isEqualTo(
                "application/json; charset=utf-8"
        );


        // Health responses should not be cached.

        assertThat(
                responseHeaders.getFirst("Cache-Control")
        ).isEqualTo("no-store");


        // Verify the response contains valid JSON.

        JsonNode json = getResponseJson();

        assertThat(json.path("status").asText())
                .isEqualTo("HEALTHY");
    }


    // =========================================================
    // Helper: Parse JSON Response
    // =========================================================

    private JsonNode getResponseJson()
            throws IOException {

        return objectMapper.readTree(
                responseBody.toByteArray()
        );
    }
}