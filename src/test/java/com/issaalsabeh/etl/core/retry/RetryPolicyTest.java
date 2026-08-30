package com.issaalsabeh.etl.core.retry;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryPolicyTest {

    @Test
    void shouldCreateValidRetryPolicy() {

        RetryPolicy policy = new RetryPolicy(
                3,
                100,
                2.0,
                5_000
        );

        assertThat(policy.maxAttempts())
                .isEqualTo(3);

        assertThat(policy.initialDelayMillis())
                .isEqualTo(100);

        assertThat(policy.backoffMultiplier())
                .isEqualTo(2.0);

        assertThat(policy.maxDelayMillis())
                .isEqualTo(5_000);
    }

    @Test
    void shouldCreateDefaultRetryPolicy() {

        RetryPolicy policy = RetryPolicy.defaultPolicy();

        assertThat(policy.maxAttempts())
                .isEqualTo(3);

        assertThat(policy.initialDelayMillis())
                .isEqualTo(100);

        assertThat(policy.backoffMultiplier())
                .isEqualTo(2.0);

        assertThat(policy.maxDelayMillis())
                .isEqualTo(5_000);
    }

    @Test
    void shouldRejectZeroMaximumAttempts() {

        assertThatThrownBy(() ->
                new RetryPolicy(
                        0,
                        100,
                        2.0,
                        5_000
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Maximum attempts must be at least 1");
    }

    @Test
    void shouldRejectNegativeMaximumAttempts() {

        assertThatThrownBy(() ->
                new RetryPolicy(
                        -1,
                        100,
                        2.0,
                        5_000
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Maximum attempts must be at least 1");
    }

    @Test
    void shouldRejectNegativeInitialDelay() {

        assertThatThrownBy(() ->
                new RetryPolicy(
                        3,
                        -1,
                        2.0,
                        5_000
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Initial delay cannot be negative");
    }

    @Test
    void shouldRejectBackoffMultiplierBelowOne() {

        assertThatThrownBy(() ->
                new RetryPolicy(
                        3,
                        100,
                        0.5,
                        5_000
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Backoff multiplier must be at least 1");
    }

    @Test
    void shouldRejectMaximumDelaySmallerThanInitialDelay() {

        assertThatThrownBy(() ->
                new RetryPolicy(
                        3,
                        1_000,
                        2.0,
                        500
                )
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Maximum delay cannot be smaller than initial delay"
                );
    }

    @Test
    void shouldCalculateExponentialBackoff() {

        RetryPolicy policy = new RetryPolicy(
                5,
                100,
                2.0,
                5_000
        );

        assertThat(policy.getDelayMillis(1))
                .isEqualTo(100);

        assertThat(policy.getDelayMillis(2))
                .isEqualTo(200);

        assertThat(policy.getDelayMillis(3))
                .isEqualTo(400);

        assertThat(policy.getDelayMillis(4))
                .isEqualTo(800);
    }

    @Test
    void shouldCapBackoffAtMaximumDelay() {

        RetryPolicy policy = new RetryPolicy(
                10,
                100,
                2.0,
                500
        );

        assertThat(policy.getDelayMillis(1))
                .isEqualTo(100);

        assertThat(policy.getDelayMillis(2))
                .isEqualTo(200);

        assertThat(policy.getDelayMillis(3))
                .isEqualTo(400);

        assertThat(policy.getDelayMillis(4))
                .isEqualTo(500);

        assertThat(policy.getDelayMillis(5))
                .isEqualTo(500);
    }
}