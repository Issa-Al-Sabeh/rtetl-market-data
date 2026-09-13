package com.issaalsabeh.etl.monitoring;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class MetricsHttpServer {

    private final PrometheusMeterRegistry registry;
    private final int port;

    private HttpServer server;

    public MetricsHttpServer(
            PrometheusMeterRegistry registry,
            int port
    ) {

        if (registry == null) {
            throw new IllegalArgumentException(
                    "Prometheus registry cannot be null"
            );
        }

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "Port must be between 1 and 65535"
            );
        }

        this.registry = registry;
        this.port = port;
    }

    public void start() {

        if (server != null) {
            throw new IllegalStateException(
                    "Metrics HTTP server is already running"
            );
        }

        try {

            server = HttpServer.create(
                    new InetSocketAddress(port),
                    0
            );

            server.createContext(
                    "/metrics",
                    this::handleMetrics
            );

            server.setExecutor(null);

            server.start();

        } catch (IOException e) {

            server = null;

            throw new IllegalStateException(
                    "Failed to start metrics HTTP server",
                    e
            );
        }
    }

    private void handleMetrics(
            HttpExchange exchange
    ) throws IOException {

        if (!"GET".equalsIgnoreCase(
                exchange.getRequestMethod()
        )) {

            exchange.sendResponseHeaders(
                    405,
                    -1
            );

            exchange.close();

            return;
        }

        byte[] response =
                registry.scrape()
                        .getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders().set(
                "Content-Type",
                "text/plain; version=0.0.4; charset=utf-8"
        );

        exchange.sendResponseHeaders(
                200,
                response.length
        );

        try (OutputStream outputStream =
                     exchange.getResponseBody()) {

            outputStream.write(response);
        }
    }

    public void stop() {

        if (server == null) {
            return;
        }

        server.stop(0);

        server = null;
    }
}