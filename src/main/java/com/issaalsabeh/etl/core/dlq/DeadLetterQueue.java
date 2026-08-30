package com.issaalsabeh.etl.core.dlq;

/**
 * Defines the contract for publishing permanently failed events
 * to a dead-letter queue.
 */
public interface DeadLetterQueue {

    /**
     * Starts the dead-letter queue publisher
     * and initializes any required resources.
     */
    void start();

    /**
     * Publishes a permanently failed event to the dead-letter queue.
     *
     * @param record the dead-letter record containing the failed event
     *               and failure information
     */
    void publish(DeadLetterRecord record);

    /**
     * Stops the dead-letter queue publisher
     * and releases any allocated resources.
     */
    void stop();
}