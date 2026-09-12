package com.issaalsabeh.etl.application;

import com.issaalsabeh.etl.connector.kafka.MarketDataProducer;
import com.issaalsabeh.etl.connector.mock.MockMarketSource;
import com.issaalsabeh.etl.model.MarketEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class MarketDataProducerApplication {

    private static final Logger logger =
            LoggerFactory.getLogger(MarketDataProducerApplication.class);

    public static void main(String[] args) {

        MockMarketSource source = new MockMarketSource();
        MarketDataProducer producer = new MarketDataProducer();

        AtomicBoolean running = new AtomicBoolean(true);

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {

                    logger.info("producer_shutdown_requested");

                    running.set(false);

                    mainThread.interrupt();

                    try {

                        mainThread.join();

                    } catch (InterruptedException e) {

                        Thread.currentThread().interrupt();

                        logger.warn(
                                "producer_shutdown_wait_interrupted"
                        );
                    }

                    logger.info("producer_stopped");
                })
        );

        boolean interrupted = false;

        try {

            source.start();

            logger.info("producer_started");

            while (running.get()) {

                MarketEvent event = source.poll();

                producer.send(event);

                logger.debug(
                        "market_event_queued eventId={} symbol={}",
                        event.eventId(),
                        event.symbol()
                );

                try {

                    Thread.sleep(1000);

                } catch (InterruptedException e) {

                    interrupted = true;
                    break;
                }
            }

        } finally {

            source.stop();

            producer.close();

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}