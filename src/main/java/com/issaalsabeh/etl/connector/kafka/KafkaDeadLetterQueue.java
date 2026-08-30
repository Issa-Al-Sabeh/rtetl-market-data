package com.issaalsabeh.etl.connector.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.issaalsabeh.etl.core.dlq.DeadLetterQueue;
import com.issaalsabeh.etl.core.dlq.DeadLetterRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDeadLetterQueue implements DeadLetterQueue {

    private final String bootstrapServers;
    private final String topic;
    private KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public KafkaDeadLetterQueue(
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
        this.objectMapper.disable(
                SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
        );
    }

    @Override
    public void start() {

        if (producer != null) {
            throw new IllegalStateException(
                    "Dead-letter queue is already running"
            );
        }

        Properties properties = new Properties();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        properties.put(
                ProducerConfig.ACKS_CONFIG,
                "all"
        );

        producer =
                new KafkaProducer<>(properties);
    }

    @Override
    public void publish(DeadLetterRecord record) {

        if (producer == null) {
            throw new IllegalStateException(
                    "Dead-letter queue has not been started"
            );
        }

        if (record == null) {
            throw new IllegalArgumentException(
                    "Dead-letter record cannot be null"
            );
        }

        try {

            String json =
                    objectMapper.writeValueAsString(record);

            ProducerRecord<String, String> kafkaRecord =
                    new ProducerRecord<>(
                            topic,
                            record.failedSink(),
                            json
                    );

            producer.send(kafkaRecord).get();

        } catch (JsonProcessingException e) {

            throw new IllegalArgumentException(
                    "Failed to serialize dead-letter record",
                    e
            );

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            throw new IllegalStateException(
                    "Interrupted while publishing dead-letter record",
                    e
            );

        } catch (ExecutionException e) {

            throw new IllegalStateException(
                    "Failed to publish dead-letter record to Kafka",
                    e
            );
        }
    }

    @Override
    public void stop() {

        if (producer != null) {
            producer.flush();
            producer.close();
            producer = null;
        }
    }
}
