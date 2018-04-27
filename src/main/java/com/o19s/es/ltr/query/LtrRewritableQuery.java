package com.o19s.es.ltr.query;

import java.util.function.Supplier;

import org.apache.lucene.search.Query;

import com.o19s.es.ltr.ranker.LtrRanker;

public interface LtrRewritableQuery {
    /**
     * Rewrite the query so that it holds the vectorSupplier
     */
    Query ltrRewrite(Supplier<LtrRanker.FeatureVector> vectorSuppler);
}
