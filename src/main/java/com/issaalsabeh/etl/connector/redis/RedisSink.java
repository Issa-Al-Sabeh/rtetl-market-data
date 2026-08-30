package com.issaalsabeh.etl.connector.redis;

import com.issaalsabeh.etl.core.Sink;
import com.issaalsabeh.etl.model.EnrichedMarketEvent;
import redis.clients.jedis.RedisClient;

public class RedisSink implements Sink<EnrichedMarketEvent> {

    private static final String LATEST_PRICES_KEY =
            "market-data:latest-prices";

    private final String latestPricesKey;

    private final String host;
    private final int port;

    private RedisClient redisClient;

    public RedisSink(
            String host,
            int port
    ){
        this(
                host,
                port,
                LATEST_PRICES_KEY
        );
    }

    public RedisSink(
            String host,
            int port,
            String latestPricesKey
    ) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException(
                    "Redis host cannot be null or blank"
            );
        }

        if (port <= 0) {
            throw new IllegalArgumentException(
                    "Redis port must be greater than zero"
            );
        }

        if (latestPricesKey == null ||
                latestPricesKey.isBlank()) {
            throw new IllegalArgumentException(
                    "Redis key cannot be null or blank"
            );
        }

        this.host = host;
        this.port = port;
        this.latestPricesKey = latestPricesKey;
    }
    @Override
    public void start() {
        if (redisClient != null) {
            throw new IllegalStateException(
                    "Sink is already running"
            );
        }
        redisClient = RedisClient.builder()
                .hostAndPort(host, port)
                .build();

    }

    @Override
    public void write(EnrichedMarketEvent data) {

        if (redisClient == null) {
            throw new IllegalStateException(
                    "Sink is not running"
            );
        }

        long result = redisClient.hset(
                latestPricesKey,
                data.symbol(),
                data.price().toPlainString()
        );
    }

    @Override
    public void stop() {

        if (redisClient != null) {
            redisClient.close();
            redisClient = null;
        }
    }

    @Override
    public Class<?> getInputType() {
        return EnrichedMarketEvent.class;
    }
}
