
package com.issaalsabeh.etl.monitoring;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricsHttpServer {

    private static final int HTTP_THREADS = 4;

    private static final int SHUTDOWN_TIMEOUT_SECONDS = 3;

    private static final AtomicInteger THREAD_COUNTER =
            new AtomicInteger();

    private final PrometheusMeterRegistry registry;

    private final int port;

    private final HealthService healthService;

    private HttpServer server;

    private ExecutorService httpExecutor;


    public MetricsHttpServer(
            PrometheusMeterRegistry registry,
            int port
    ) {

        this(registry, port, null);
    }


    public MetricsHttpServer(
            PrometheusMeterRegistry registry,
            int port,
            HealthService healthService
    ) {

        if (registry == null) {
            throw new IllegalArgumentException(
                    "Prometheus registry cannot be null"
            );
        }

        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException(
                    "Port must be between 0 and 65535"
            );
        }

        this.registry = registry;
        this.port = port;
        this.healthService = healthService;
    }


    public synchronized void start() {

        if (server != null) {
            throw new IllegalStateException(
                    "Metrics HTTP server is already running"
            );
        }

        HttpServer newServer = null;

        ExecutorService newExecutor = null;

        try {

            newServer = HttpServer.create(
                    new InetSocketAddress(port),
                    0
            );

            newServer.createContext(
                    "/metrics",
                    this::handleMetrics
            );

            if (healthService != null) {

                newServer.createContext(
                        "/health",
                        new HealthHttpHandler(
                                healthService
                        )
                );
            }

            newExecutor =
                    Executors.newFixedThreadPool(
                            HTTP_THREADS,
                            task -> {

                                Thread thread = new Thread(
                                        task,
                                        "rtetl-monitoring-http-"
                                                + THREAD_COUNTER
                                                .incrementAndGet()
                                );

                                thread.setDaemon(true);

                                return thread;
                            }
                    );

            newServer.setExecutor(newExecutor);

            newServer.start();

            server = newServer;

            httpExecutor = newExecutor;

        } catch (IOException | RuntimeException e) {

            if (newServer != null) {
                newServer.stop(0);
            }

            if (newExecutor != null) {
                newExecutor.shutdownNow();
            }

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


    public synchronized int getBoundPort() {

        if (server == null) {
            throw new IllegalStateException(
                    "HTTP server is not running"
            );
        }

        return server.getAddress().getPort();
    }


    public synchronized void stop() {

        if (server == null) {
            return;
        }

        server.stop(0);

        server = null;

        if (httpExecutor != null) {

            httpExecutor.shutdown();

            try {

                if (!httpExecutor.awaitTermination(
                        SHUTDOWN_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS
                )) {

                    httpExecutor.shutdownNow();
                }

            } catch (InterruptedException e) {

                httpExecutor.shutdownNow();

                Thread.currentThread().interrupt();

            } finally {

                httpExecutor = null;
            }
        }
    }
}