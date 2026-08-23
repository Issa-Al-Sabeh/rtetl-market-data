package com.issaalsabeh.etl.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineExecutor<T> {

    private static final Logger logger =
            LoggerFactory.getLogger(PipelineExecutor.class);

    private final Pipeline<T> pipeline;

    private volatile boolean running;

    public PipelineExecutor(Pipeline<T> pipeline) {
        if (pipeline == null) {
            throw new IllegalArgumentException("Pipeline cannot be null");
        }

        this.pipeline = pipeline;
        this.running = false;
    }

    public void start() {
        pipeline.validate();

        pipeline.getSource().start();

        for (Sink<?> sink : pipeline.getSinks()) {
            sink.start();
        }

        running = true;

        try{
            while (running) {
                T event = pipeline.getSource().poll();

                if (event == null) {
                    continue;
                }

                Object current = event;

                try {
                    for (Transformer<?, ?> transformer : pipeline.getTransformers()) {
                        @SuppressWarnings("unchecked")
                        Transformer<Object, Object> typedTransformer =
                                (Transformer<Object, Object>) transformer;

                        current = typedTransformer.transform(current);
                    }
                } catch (Exception e) {
                    logger.error("Failed to transform event: {}", current, e);
                    continue;
                }

                for (Sink<?> sink : pipeline.getSinks()) {

                    try{
                        @SuppressWarnings("unchecked")
                        Sink<Object> typedSink = (Sink<Object>) sink;
                        typedSink.write(current);
                    }catch (Exception e){
                        logger.error("Failed to write event {} to sink {}",
                                current,
                                sink.getClass().getSimpleName(),
                                e
                        );
                    }
                }
            }
        }finally {
            pipeline.getSource().stop();

            for (Sink<?> sink: pipeline.getSinks()){
                sink.stop();
            }

            running = false;
        }
    }

    public void stop(){
        running = false;
    }
}