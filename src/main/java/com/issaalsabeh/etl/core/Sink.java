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

    /**
     * Returns the runtime type expected as input by this transformer.
     * This is used to validate type compatibility with the previous
     * stage in the pipeline.
     *
     * @return the class representing the input data type
     */
    Class<?> getInputType();
}