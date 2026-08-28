package com.issaalsabeh.etl.core;

/**
 * Transforms an input value into an output value.
 *
 * @param <I> the type of input accepted by the transformer
 * @param <O> the type of output produced by the transformer
 */
public interface Transformer<I, O> {

    /**
     * Transforms the given input into an output value.
     *
     * @param input the value to transform
     * @return the transformed output
     */
    O transform(I input);

    /**
     * Returns the runtime type expected as input by this transformer.
     * This is used to validate type compatibility with the previous
     * stage in the pipeline.
     *
     * @return the class representing the input data type
     */
    default Class<?> getInputType() {
        return Object.class;
    }

    /**
     * Returns the runtime type produced by this transformer.
     * This is used to validate type compatibility with the next
     * stage in the pipeline.
     *
     * @return the class representing the output data type
     */
    default Class<?> getOutputType() {
        return Object.class;
    }
}