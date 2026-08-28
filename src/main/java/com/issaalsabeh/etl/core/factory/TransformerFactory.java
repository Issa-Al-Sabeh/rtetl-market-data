package com.issaalsabeh.etl.core.factory;

import com.issaalsabeh.etl.core.Transformer;
import com.issaalsabeh.etl.transformations.EnrichmentTransformer;
import com.issaalsabeh.etl.transformations.PriceNormalizationTransformer;
import com.issaalsabeh.etl.transformations.ValidationTransformer;

public final class TransformerFactory {

    private TransformerFactory() {
    }

    public static Transformer<?, ?> create(String transformer) {

        if (transformer == null || transformer.isBlank()) {
            throw new IllegalArgumentException(
                    "Transformer config cannot be null or empty"
            );
        }

        if (transformer.equalsIgnoreCase("validate")) {
            return new ValidationTransformer();
        }

        if (transformer.equalsIgnoreCase("normalize")) {
            return new PriceNormalizationTransformer();
        }

        if (transformer.equalsIgnoreCase("enrich")) {
            return new EnrichmentTransformer();
        }

        throw new IllegalArgumentException(
                "Unsupported transformer type: " + transformer
        );
    }
}