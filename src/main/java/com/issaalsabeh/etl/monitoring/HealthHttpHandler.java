
package com.issaalsabeh.etl.monitoring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;

public class HealthHttpHandler implements HttpHandler {

    private static final String CONTENT_TYPE =
            "application/json; charset=utf-8";

    private final HealthService healthService;

    private final ObjectMapper objectMapper;


    // =========================================================
    // Constructor
    // =========================================================

    public HealthHttpHandler(HealthService healthService) {

        this.healthService = Objects.requireNonNull(
                healthService,
                "Health service must not be null"
        );

        this.objectMapper = new ObjectMapper();
    }


    // =========================================================
    // Handle HTTP Request
    // =========================================================

    @Override
    public void handle(HttpExchange exchange)
            throws IOException {

        Objects.requireNonNull(
                exchange,
                "HTTP exchange must not be null"
        );


        // -----------------------------------------------------
        // Allow GET Requests Only
        // -----------------------------------------------------

        if (!"GET".equals(exchange.getRequestMethod())) {

            exchange.getResponseHeaders().set(
                    "Allow",
                    "GET"
            );

            exchange.sendResponseHeaders(
                    405,
                    -1
            );

            exchange.close();

            return;
        }


        // -----------------------------------------------------
        // Execute Health Checks
        // -----------------------------------------------------

        HealthReport report;

        int statusCode;

        try {

            report = healthService.checkHealth();

            statusCode =
                    report.status() == HealthStatus.UNHEALTHY
                            ? 503
                            : 200;

        } catch (Exception e) {

            // Unexpected internal failure.
            // Do not expose exception details to HTTP clients.

            report = new HealthReport(
                    HealthStatus.UNHEALTHY,
                    Map.of(
                            "application",
                            new ComponentHealth(
                                    HealthStatus.UNHEALTHY
                            )
                    )
            );

            statusCode = 503;
        }


        // -----------------------------------------------------
        // Serialize Health Report
        // -----------------------------------------------------

        byte[] responseBody =
                objectMapper.writeValueAsBytes(report);


        // -----------------------------------------------------
        // Set Response Headers
        // -----------------------------------------------------

        exchange.getResponseHeaders().set(
                "Content-Type",
                CONTENT_TYPE
        );

        exchange.getResponseHeaders().set(
                "Cache-Control",
                "no-store"
        );


        // -----------------------------------------------------
        // Send HTTP Response
        // -----------------------------------------------------

        exchange.sendResponseHeaders(
                statusCode,
                responseBody.length
        );


        try (OutputStream outputStream =
                     exchange.getResponseBody()) {

            outputStream.write(responseBody);
        }
    }
}