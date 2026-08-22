package com.issaalsabeh.etl.core;

/**
 * Represents a source of data for the ETL pipeline.
 *
 * @param <T> the type of data produced by this source
 */
public interface Source<T> {

    /**
     * Starts the source and initializes required resources.
     */
    void start();

    /**
     * Retrieves the next available item from the source.
     *
     * @return the next item
     */
    T poll();

    /**
     * Stops the source and releases its resources.
     */
    void stop();
}