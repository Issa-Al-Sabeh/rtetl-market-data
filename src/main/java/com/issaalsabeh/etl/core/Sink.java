package com.issaalsabeh.etl.core;

/**
 * Represents a destination that receives and writes data from the ETL pipeline.
 *
 * @param <T> the type of data accepted by the sink
 */
public interface Sink<T> {

    /**
     * Starts the sink and initializes any resources required for writing data.
     */
    void start();

    /**
     * Writes the given data to the sink.
     *
     * @param data the data to write
     */
    void write(T data);

    /**
     * Stops the sink and releases any resources used by it.
     */
    void stop();
}