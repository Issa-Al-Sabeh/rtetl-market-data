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

    /**
     * Returns the runtime type of data produced by this source.
     * This is used to validate type compatibility between the source
     * and the next stage in the pipeline.
     *
     * @return the class representing the output data type
     */
    Class<?> getOutputType();
}