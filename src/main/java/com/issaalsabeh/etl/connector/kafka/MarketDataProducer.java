package com.issaalsabeh.etl.connector.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.issaalsabeh.etl.model.MarketEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MarketDataProducer {
    private static final String TOPIC = "market-data";

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public MarketDataProducer() {

        Properties properties = new Properties();

        properties.put(
                "bootstrap.servers",
                "localhost:9092"
        );

        properties.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        properties.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        producer = new KafkaProducer<>(properties);
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();

    }

    public void send(MarketEvent event) {

        try {
            String json = objectMapper.writeValueAsString(event);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            TOPIC,
                            event.symbol(),
                            json
                    );

            producer.send(record);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    "Failed to serialize MarketEvent",
                    e
            );
        }
    }

    public void close() {
        producer.flush();
        producer.close();
    }
}
