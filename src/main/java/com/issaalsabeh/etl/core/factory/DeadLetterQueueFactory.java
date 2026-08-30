package com.issaalsabeh.etl.core.factory;

import com.issaalsabeh.etl.config.DeadLetterQueueConfig;
import com.issaalsabeh.etl.connector.kafka.KafkaDeadLetterQueue;
import com.issaalsabeh.etl.core.dlq.DeadLetterQueue;

public final class DeadLetterQueueFactory {

    private DeadLetterQueueFactory() {
    }

    public static DeadLetterQueue create(
            DeadLetterQueueConfig config
    ) {

        if (config == null) {
            throw new IllegalArgumentException(
                    "Dead letter queue config cannot be null"
            );
        }

        String type = config.getType();

        if (type.equalsIgnoreCase("kafka")) {

            String bootstrapServers =
                    config.getProperty("bootstrap.servers");

            String topic =
                    config.getProperty("topic");

            return new KafkaDeadLetterQueue(
                    bootstrapServers,
                    topic
            );
        }

        throw new IllegalArgumentException(
                "Unsupported dead letter queue type: " + type
        );
    }
}