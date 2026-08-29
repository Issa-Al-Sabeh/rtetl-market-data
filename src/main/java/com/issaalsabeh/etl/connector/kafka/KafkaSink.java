package com.issaalsabeh.etl.connector.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.issaalsabeh.etl.core.Sink;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaSink implements Sink<EnrichedMarketEvent> {

    private static final Logger logger =
            LoggerFactory.getLogger(KafkaSink.class);
    private final String bootstrapServers;
    private final String topic;

    private KafkaProducer<String, String> kafkaProducer;
    private final ObjectMapper objectMapper;

    public KafkaSink(
            String bootstrapServers,
            String topic
    ) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException(
                    "Bootstrap servers cannot be null or blank"
            );
        }

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException(
                    "Topic cannot be null or blank"
            );
        }

        this.bootstrapServers = bootstrapServers;
        this.topic = topic;

        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
    }

    @Override
    public void start() {

        if (kafkaProducer != null) {
            throw new IllegalStateException(
                    "Sink is already running"
            );
        }

        Properties properties = new Properties();

        properties.put(
                "bootstrap.servers",
                bootstrapServers
        );

        properties.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        properties.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        kafkaProducer = new KafkaProducer<>(properties);
    }

    @Override
    public void write(EnrichedMarketEvent data) {

        if (kafkaProducer == null) {
            throw new IllegalStateException(
                    "Sink is not running"
            );
        }

        try {
            String json =
                    objectMapper.writeValueAsString(data);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            topic,
                            data.symbol(),
                            json
                    );

            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error(
                            "Failed to publish event {} to Kafka topic {}",
                            data.eventId(),
                            topic,
                            exception
                    );
                }
            });

        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "Failed to serialize enriched market event: "
                            + data.eventId(),
                    e
            );
        }
    }

    @Override
    public void stop() {

        if (kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close();
            kafkaProducer = null;
        }
    }

    @Override
    public Class<?> getInputType() {
        return EnrichedMarketEvent.class;
    }
}