package com.issaalsabeh.etl.application;

import com.issaalsabeh.etl.connector.kafka.MarketDataProducer;
import com.issaalsabeh.etl.connector.mock.MockMarketSource;
import com.issaalsabeh.etl.model.MarketEvent;

import java.util.concurrent.atomic.AtomicBoolean;

public class MarketDataProducerApplication {

    public static void main(String[] args) {

        MockMarketSource source = new MockMarketSource();
        MarketDataProducer producer = new MarketDataProducer();

        AtomicBoolean running = new AtomicBoolean(true);

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {

                    System.out.println("Shutting down producer...");

                    running.set(false);

                    // Wake the main thread if it is sleeping.
                    mainThread.interrupt();

                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    System.out.println("Producer stopped.");
                })
        );

        boolean interrupted = false;

        try {

            source.start();

            while (running.get()) {

                MarketEvent event = source.poll();

                producer.send(event);

                System.out.println("Published: " + event);

                try {

                    Thread.sleep(1000);

                } catch (InterruptedException e) {

                    interrupted = true;
                    break;
                }
            }

        } finally {

            source.stop();

            /*
             * Close Kafka BEFORE restoring the interrupt flag.
             */
            producer.close();

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}