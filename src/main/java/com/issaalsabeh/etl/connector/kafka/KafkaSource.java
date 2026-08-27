package com.issaalsabeh.etl.connector.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.issaalsabeh.etl.connector.file.FileSource;
import com.issaalsabeh.etl.core.Source;
import com.issaalsabeh.etl.model.MarketEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

public class KafkaSource implements Source<MarketEvent> {
    private final String topic;
    KafkaConsumer<String, String> kafkaConsumer;
    private final Queue<ConsumerRecord<String, String>> queue = new LinkedList<>();
    private final ObjectMapper objectMapper;
    private static final Logger logger =
            LoggerFactory.getLogger(KafkaSource.class);

    public KafkaSource(){
        this(
                "localhost:9092",
                "market-data",
                "market-data-etl"
        );
    }

    public KafkaSource(
            String bootstrapServers,
            String topic,
            String groupId
    ){
        this.topic = topic;

        Properties properties = new Properties();

        properties.put(
                "bootstrap.servers",
                bootstrapServers
        );

        properties.put(
                "key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        properties.put(
                "value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        properties.put(
                "group.id",
                groupId
        );

        kafkaConsumer = new KafkaConsumer<>(properties);

        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
    }
    @Override
    public void start() {
        kafkaConsumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public MarketEvent poll() {

        if (queue.isEmpty()){
            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                queue.add(record);
            }
        }

        ConsumerRecord<String, String> record = queue.poll();

        if (record == null) {
            return null;
        }

        try {
            return objectMapper.readValue(
                    record.value(),
                    MarketEvent.class
            );
        } catch (JsonProcessingException e) {
            logger.warn(
                    "Skipping malformed Kafka message: {}",
                    record.value()
            );

            return null;
        }
    }

    @Override
    public void stop() {
        kafkaConsumer.close();
    }
}
