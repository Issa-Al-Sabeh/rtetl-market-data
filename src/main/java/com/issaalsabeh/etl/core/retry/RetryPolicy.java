package com.issaalsabeh.etl.core.retry;

/**
 * Defines the retry behavior used when an operation fails.
 *
 * @param maxAttempts        maximum number of total attempts,
 *                           including the initial attempt
 * @param initialDelayMillis delay before the first retry
 * @param backoffMultiplier  multiplier used to increase the retry delay
 * @param maxDelayMillis     maximum allowed retry delay
 */
public record RetryPolicy(
        int maxAttempts,
        long initialDelayMillis,
        double backoffMultiplier,
        long maxDelayMillis
) {

    public RetryPolicy {

        if (maxAttempts < 1) {
            throw new IllegalArgumentException(
                    "Maximum attempts must be at least 1"
            );
        }

        if (initialDelayMillis < 0) {
            throw new IllegalArgumentException(
                    "Initial delay cannot be negative"
            );
        }

        if (backoffMultiplier < 1) {
            throw new IllegalArgumentException(
                    "Backoff multiplier must be at least 1"
            );
        }

        if (maxDelayMillis < initialDelayMillis) {
            throw new IllegalArgumentException(
                    "Maximum delay cannot be smaller than initial delay"
            );
        }
    }

    /**
     * Creates the default retry policy.
     *
     * @return the default retry policy
     */
    public static RetryPolicy defaultPolicy() {
        return new RetryPolicy(
                3,
                100,
                2.0,
                5_000
        );
    }

    /**
     * Calculates the delay to wait after a failed attempt before retrying.
     *
     * <p>The delay grows exponentially based on the configured
     * {@code initialDelayMillis} and {@code backoffMultiplier}.
     * The calculated delay is capped at {@code maxDelayMillis}.</p>
     *
     * @param failedAttempt the number of the attempt that just failed,
     *                      starting from 1
     * @return the delay in milliseconds before the next retry
     */
    public long getDelayMillis(int failedAttempt) {

        double delay =
                initialDelayMillis
                        * Math.pow(
                        backoffMultiplier,
                        failedAttempt - 1
                );

        return Math.min(
                (long) delay,
                maxDelayMillis
        );
    }
}