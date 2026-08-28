package com.issaalsabeh.etl.core.factory;

import com.issaalsabeh.etl.core.Transformer;
import com.issaalsabeh.etl.transformations.EnrichmentTransformer;
import com.issaalsabeh.etl.transformations.PriceNormalizationTransformer;
import com.issaalsabeh.etl.transformations.ValidationTransformer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransformerFactoryTest {

    @Test
    void shouldCreateValidationTransformer() {

        Transformer<?, ?> transformer =
                TransformerFactory.create("validate");

        assertThat(transformer)
                .isInstanceOf(ValidationTransformer.class);
    }

    @Test
    void shouldCreatePriceNormalizationTransformer() {

        Transformer<?, ?> transformer =
                TransformerFactory.create("normalize");

        assertThat(transformer)
                .isInstanceOf(PriceNormalizationTransformer.class);
    }

    @Test
    void shouldCreateEnrichmentTransformer() {

        Transformer<?, ?> transformer =
                TransformerFactory.create("enrich");

        assertThat(transformer)
                .isInstanceOf(EnrichmentTransformer.class);
    }

    @Test
    void shouldIgnoreCaseWhenCreatingTransformer() {

        Transformer<?, ?> transformer =
                TransformerFactory.create("VALIDATE");

        assertThat(transformer)
                .isInstanceOf(ValidationTransformer.class);
    }

    @Test
    void shouldRejectNullTransformerType() {

        assertThatThrownBy(() ->
                TransformerFactory.create(null)
        )
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectBlankTransformerType() {

        assertThatThrownBy(() ->
                TransformerFactory.create("   ")
        )
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectUnsupportedTransformerType() {

        assertThatThrownBy(() ->
                TransformerFactory.create("unknown")
        )
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported transformer type");
    }
}