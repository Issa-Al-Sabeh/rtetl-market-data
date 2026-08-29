package com.issaalsabeh.etl;

import com.issaalsabeh.etl.config.PipelineConfig;
import com.issaalsabeh.etl.config.PipelineConfigLoader;
import com.issaalsabeh.etl.core.Pipeline;
import com.issaalsabeh.etl.core.PipelineExecutor;
import com.issaalsabeh.etl.core.factory.PipelineFactory;

public class Main {

    public static void main(String[] args) {

        PipelineConfigLoader loader =
                new PipelineConfigLoader();

        PipelineConfig config =
                loader.load();

        Pipeline<?> pipeline =
                PipelineFactory.create(config);

        PipelineExecutor<?> executor =
                new PipelineExecutor<>(pipeline);

        Runtime.getRuntime().addShutdownHook(
                new Thread(executor::stop)
        );

        executor.start();
    }
}