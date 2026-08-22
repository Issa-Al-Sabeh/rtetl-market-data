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
}