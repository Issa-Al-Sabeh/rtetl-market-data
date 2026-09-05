package com.issaalsabeh.etl.connector.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.issaalsabeh.etl.core.CommittableSource;
import com.issaalsabeh.etl.model.MarketEvent;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class KafkaSource implements CommittableSource<MarketEvent> {
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final String autoOffsetReset;
    private KafkaConsumer<String, String> kafkaConsumer;
    private final Queue<ConsumerRecord<String, String>> queue = new LinkedList<>();
    private final ObjectMapper objectMapper;
    private static final Logger logger =
            LoggerFactory.getLogger(KafkaSource.class);

    private ConsumerRecord<String, String> currentRecord;

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
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.autoOffsetReset = "latest";

        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
    }

    public KafkaSource(
            String bootstrapServers,
            String topic,
            String groupId,
            String autoOffsetReset
    ) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.autoOffsetReset = autoOffsetReset;

        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
    }

    @Override
    public void start() {

        Properties properties = new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId
        );

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                autoOffsetReset
        );

        properties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public MarketEvent poll() {

        if (kafkaConsumer == null) {
            throw new IllegalStateException(
                    "Source is not running"
            );
        }

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

            MarketEvent event = objectMapper.readValue(
                    record.value(),
                    MarketEvent.class
            );

            currentRecord = record;

            return event;
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

        if (kafkaConsumer != null) {
            kafkaConsumer.close();
            kafkaConsumer = null;
        }
    }

    @Override
    public Class<?> getOutputType() {
        return MarketEvent.class;
    }

    @Override
    public void commit() {

        if (kafkaConsumer == null) {
            throw new IllegalStateException(
                    "Kafka source has not been started"
            );
        }

        if (currentRecord == null) {
            throw new IllegalStateException(
                    "There is no currently processed record to commit"
            );
        }

        TopicPartition topicPartition =
                new TopicPartition(
                        currentRecord.topic(),
                        currentRecord.partition()
                );

        OffsetAndMetadata offset =
                new OffsetAndMetadata(
                        currentRecord.offset() + 1
                );

        kafkaConsumer.commitSync(
                Map.of(topicPartition, offset)
        );

        currentRecord = null;
    }
}
