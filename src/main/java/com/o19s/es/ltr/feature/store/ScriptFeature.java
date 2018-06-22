package com.o19s.es.ltr.feature.store;

import com.o19s.es.ltr.LtrQueryContext;
import com.o19s.es.ltr.feature.Feature;
import com.o19s.es.ltr.feature.FeatureSet;
import com.o19s.es.ltr.query.FeatureVectorWeight;
import com.o19s.es.ltr.ranker.LtrRanker;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScriptScoreFunction;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public class ScriptFeature implements Feature {
    private final String name;
    private final Script script;

    public ScriptFeature(String name, Script script) {
        this.name = Objects.requireNonNull(name);
        this.script = Objects.requireNonNull(script);
    }

    public static ScriptFeature compile(StoredFeature feature) throws IOException {
        return new ScriptFeature(feature.name(),
                Script.parse(XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, feature.template())));
    }

    /**
     * The feature name
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * Transform this feature into a lucene query
     *
     * @param context
     * @param set
     * @param params
     */
    @Override
    public Query doToQuery(LtrQueryContext context, FeatureSet set, Map<String, Object> params) {
        Map<String, Object> nparams = new HashMap<>(params);
        SupplierSupplier vector = new SupplierSupplier();
        nparams.put("feature_vector", vector);
        nparams.putAll(script.getParams());
        Script nScript = new Script(this.script.getType(), this.script.getLang(), this.script.getIdOrCode(), this.script.getOptions(), nparams);
        SearchScript.Factory searchScript = context.getQueryShardContext().getScriptService().compile(script, SearchScript.CONTEXT);
        return new LtrScript(new ScriptScoreFunction(script, searchScript.newFactory(nparams, context.getQueryShardContext().lookup())), vector);
    }

    static class LtrScript extends Query {
        private final ScriptScoreFunction scoreFunction;
        private final SupplierSupplier vector;

        LtrScript(ScriptScoreFunction scoreFunction, SupplierSupplier vector) {
            this.scoreFunction = scoreFunction;
            this.vector = vector;
        }

        @Override
        public String toString(String field) {
            return "TODO";
        }

        @Override
        public boolean equals(Object obj) {
            // TODO:
            return false;
        }

        @Override
        public int hashCode() {
            //TODO:
            return 0;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
            return super.createWeight(searcher, needsScores, boost);
        }

        class LtrScriptWeight extends FeatureVectorWeight {

            protected LtrScriptWeight(Query query) {
                super(query);
            }

            @Override
            public Explanation explain(LeafReaderContext context, LtrRanker.FeatureVector vector, int doc) throws IOException {
                LtrScript.this.vector.set(() -> vector);
                // TODO; figure out why we need to pass an explanation here
                return scoreFunction.getLeafScoreFunction(context).explainScore(doc, Explanation.match(1F, "TODO"));
            }

            @Override
            public Scorer scorer(LeafReaderContext context, Supplier<LtrRanker.FeatureVector> vectorSupplier) throws IOException {
                LtrScript.this.vector.set(vectorSupplier);
                LeafScoreFunction leafScoreFunction = scoreFunction.getLeafScoreFunction(context);
                DocIdSetIterator iterator = DocIdSetIterator.all(context.reader().maxDoc());
                return new Scorer(this) {
                    @Override
                    public int docID() {
                        return iterator.docID();
                    }

                    @Override
                    public float score() throws IOException {
                        return (float) leafScoreFunction.score(iterator.docID(), 0F);
                    }

                    @Override
                    public DocIdSetIterator iterator() {
                        return iterator;
                    }
                };
            }

            @Override
            public void extractTerms(Set<Term> terms) {
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        }
    }


    static class SupplierSupplier implements Supplier<LtrRanker.FeatureVector> {
        private Supplier<LtrRanker.FeatureVector> vectorSupplier;
        @Override
        public LtrRanker.FeatureVector get() {
            return vectorSupplier.get();
        }

        public void set(Supplier<LtrRanker.FeatureVector> supplier) {
            this.vectorSupplier = supplier;
        }
    }
}
