package com.issaalsabeh.etl.connector.redis;

import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisSinkIntegrationTest {

    private static final String HOST = "localhost";
    private static final int PORT = 6379;
    private String testKey;

    private RedisClient redisClient;

    @BeforeEach
    void setUp() {

        testKey =
                "market-data:test:latest-prices:"
                        + UUID.randomUUID();

        redisClient = RedisClient.builder()
                .hostAndPort(HOST, PORT)
                .build();

    }

    @AfterEach
    void tearDown() {

        if (redisClient != null) {
            redisClient.del(testKey);
            redisClient.close();
        }
    }

    @Test
    void shouldStoreLatestPriceForSymbol() {

        RedisSink sink =
                new RedisSink(
                        HOST,
                        PORT,
                        testKey
                );

        EnrichedMarketEvent event =
                new EnrichedMarketEvent(
                        UUID.randomUUID(),
                        "AAPL",
                        new BigDecimal("150.2500"),
                        100,
                        Instant.now(),
                        new BigDecimal("15025.0000")
                );

        try {
            sink.start();

            sink.write(event);

            String storedPrice =
                    redisClient.hget(
                            testKey,
                            event.symbol()
                    );

            assertThat(storedPrice)
                    .isEqualTo(
                            event.price().toPlainString()
                    );

        } finally {
            sink.stop();
        }
    }

    @Test
    void shouldOverwritePreviousPriceForSameSymbol() {

        RedisSink sink =
                new RedisSink(
                        HOST,
                        PORT,
                        testKey
                );

        EnrichedMarketEvent firstEvent =
                new EnrichedMarketEvent(
                        UUID.randomUUID(),
                        "AAPL",
                        new BigDecimal("150.2500"),
                        100,
                        Instant.now(),
                        new BigDecimal("15025.0000")
                );

        EnrichedMarketEvent secondEvent =
                new EnrichedMarketEvent(
                        UUID.randomUUID(),
                        "AAPL",
                        new BigDecimal("175.5000"),
                        200,
                        Instant.now(),
                        new BigDecimal("35100.0000")
                );

        try {
            sink.start();

            sink.write(firstEvent);
            sink.write(secondEvent);

            String storedPrice =
                    redisClient.hget(
                            testKey,
                            "AAPL"
                    );

            assertThat(storedPrice)
                    .isEqualTo("175.5000");

        } finally {
            sink.stop();
        }
    }
}