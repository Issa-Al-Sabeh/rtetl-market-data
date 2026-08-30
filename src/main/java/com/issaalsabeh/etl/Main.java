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

public class Main {

    public static void main(String[] args) {

        PipelineConfigLoader configLoader =
                new PipelineConfigLoader();

        PipelineConfig config =
                configLoader.load();

        Pipeline<?> pipeline =
                PipelineFactory.create(config);

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
                        deadLetterQueue
                );

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(executor::stop)
                );

        executor.start();
    }
}