
package com.issaalsabeh.etl.monitoring;

import redis.clients.jedis.RedisClient;
import redis.clients.jedis.DefaultJedisClientConfig;

public class RedisHealthCheck implements HealthCheck {

    private static final int TIMEOUT_MS = 2000;

    private final String host;
    private final int port;


    public RedisHealthCheck(
            String host,
            int port
    ) {

        if (host == null || host.isBlank()) {

            throw new IllegalArgumentException(
                    "Redis host must not be null or blank"
            );
        }

        if (port < 1 || port > 65535) {

            throw new IllegalArgumentException(
                    "Redis port must be between 1 and 65535"
            );
        }

        this.host = host;
        this.port = port;
    }


    @Override
    public String name() {
        return "redis";
    }


    @Override
    public boolean isCritical() {
        return false;
    }


    @Override
    public void check() throws Exception {

        DefaultJedisClientConfig clientConfig =
                DefaultJedisClientConfig.builder()
                        .connectionTimeoutMillis(TIMEOUT_MS)
                        .socketTimeoutMillis(TIMEOUT_MS)
                        .build();


        try (RedisClient client =
                     RedisClient.builder()
                             .hostAndPort(host, port)
                             .clientConfig(clientConfig)
                             .build()) {

            String response = client.ping();

            if (!"PONG".equals(response)) {

                throw new IllegalStateException(
                        "Redis health check returned an unexpected response"
                );
            }
        }
    }
}