
package com.issaalsabeh.etl;

import com.issaalsabeh.etl.config.DeadLetterQueueConfig;
import com.issaalsabeh.etl.config.PipelineConfig;
import com.issaalsabeh.etl.config.PipelineConfigLoader;
import com.issaalsabeh.etl.core.Pipeline;
import com.issaalsabeh.etl.core.PipelineExecutor;
import com.issaalsabeh.etl.core.dlq.DeadLetterQueue;
import com.issaalsabeh.etl.core.factory.DeadLetterQueueFactory;
import com.issaalsabeh.etl.core.factory.PipelineFactory;
import com.issaalsabeh.etl.core.retry.RetryPolicy;
import com.issaalsabeh.etl.monitoring.HealthCheck;
import com.issaalsabeh.etl.monitoring.HealthCheckFactory;
import com.issaalsabeh.etl.monitoring.HealthService;
import com.issaalsabeh.etl.monitoring.MetricsHttpServer;
import com.issaalsabeh.etl.monitoring.PipelineMetrics;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.util.List;

public class Main {

    private static final int METRICS_PORT = 8080;

    public static void main(String[] args) {

        PipelineConfigLoader configLoader =
                new PipelineConfigLoader();

        PipelineConfig config =
                configLoader.load();

        Pipeline<?> pipeline =
                PipelineFactory.create(config);

        PrometheusMeterRegistry prometheusRegistry =
                new PrometheusMeterRegistry(
                        PrometheusConfig.DEFAULT
                );

        PipelineMetrics pipelineMetrics =
                new PipelineMetrics(
                        prometheusRegistry,
                        pipeline.getName()
                );

        PipelineConfig.ConnectorConfig dlqConnector =
                config.getPipeline()
                        .getDeadLetterQueue();

        DeadLetterQueueConfig dlqConfig =
                new DeadLetterQueueConfig(
                        dlqConnector.getType(),
                        dlqConnector.getProperties()
                );

        DeadLetterQueue deadLetterQueue =
                DeadLetterQueueFactory.create(
                        dlqConfig
                );

        PipelineExecutor<?> executor =
                new PipelineExecutor<>(
                        pipeline,
                        RetryPolicy.defaultPolicy(),
                        deadLetterQueue,
                        pipelineMetrics
                );

        List<HealthCheck> healthChecks =
                HealthCheckFactory.create(config);

        HealthService healthService =
                new HealthService(
                        executor::getState,
                        healthChecks
                );

        MetricsHttpServer metricsServer =
                new MetricsHttpServer(
                        prometheusRegistry,
                        METRICS_PORT,
                        healthService
                );

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(() -> {

                            executor.stop();

                            try {

                                executor.awaitTermination();

                            } catch (InterruptedException e) {

                                Thread.currentThread().interrupt();
                            }
                        })
                );

        try {

            metricsServer.start();

            executor.start();

        } finally {

            metricsServer.stop();
        }
    }
}