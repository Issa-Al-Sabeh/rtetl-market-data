package com.issaalsabeh.etl.application;

import com.issaalsabeh.etl.connector.kafka.MarketDataProducer;
import com.issaalsabeh.etl.connector.mock.MockMarketSource;
import com.issaalsabeh.etl.model.MarketEvent;

public class MarketDataProducerApplication {
    public static void main(String[] args) {
        MockMarketSource source = new MockMarketSource();
        MarketDataProducer producer = new MarketDataProducer();

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    System.out.println("Shutting down producer...");

                    source.stop();
                    producer.close();

                    System.out.println("Producer stopped.");
                })
        );

        source.start();

        while (true){
            MarketEvent event = source.poll();

            producer.send(event);

            System.out.println("Published: " + event);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

    }
}
