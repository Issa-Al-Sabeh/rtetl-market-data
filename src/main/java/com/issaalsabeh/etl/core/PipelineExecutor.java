package com.issaalsabeh.etl.core;

import com.issaalsabeh.etl.core.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineExecutor<T> {

    private static final Logger logger =
            LoggerFactory.getLogger(PipelineExecutor.class);

    private final Pipeline<T> pipeline;

    private volatile boolean running;

    private final RetryPolicy retryPolicy;

    public PipelineExecutor(Pipeline<T> pipeline) {
        this(
                pipeline,
                RetryPolicy.defaultPolicy()
        );
    }

    public PipelineExecutor(
            Pipeline<T> pipeline,
            RetryPolicy retryPolicy
    ) {
        if (pipeline == null) {
            throw new IllegalArgumentException(
                    "Pipeline cannot be null"
            );
        }

        if (retryPolicy == null) {
            throw new IllegalArgumentException(
                    "Retry policy cannot be null"
            );
        }

        this.pipeline = pipeline;
        this.retryPolicy = retryPolicy;
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

                        writeWithRetry(
                                typedSink,
                                current
                        );
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

    private void writeWithRetry(
            Sink<Object> sink,
            Object event
    ) {
        for (int attempt = 1;
             attempt <= retryPolicy.maxAttempts();
             attempt++) {

            try {
                sink.write(event);
                return;

            } catch (Exception e) {

                if (attempt == retryPolicy.maxAttempts()) {
                    logger.error(
                            "Sink {} failed after {} attempts",
                            sink.getClass().getSimpleName(),
                            retryPolicy.maxAttempts(),
                            e
                    );
                    return;
                }

                long delay = retryPolicy.getDelayMillis(attempt);

                logger.warn(
                        "Sink {} failed on attempt {}/{}. Retrying in {} ms",
                        sink.getClass().getSimpleName(),
                        attempt,
                        retryPolicy.maxAttempts(),
                        delay,
                        e
                );

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}