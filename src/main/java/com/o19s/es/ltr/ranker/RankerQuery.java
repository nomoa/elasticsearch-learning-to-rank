package com.o19s.es.ltr.ranker;

import com.o19s.es.ltr.query.NoopScorer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Lucene query designed to apply a ranking model provided by {@link LtrRanker}
 * This query is not designed for retrieval, in other words it will score
 * all the docs in the index and thus must be used either in a rescore phase
 * or within a BooleanQuery and an appropriate filter clause.
 */
public class RankerQuery extends Query {
    private final Query[] queries;
    private final Feature[] features;
    private final LtrRanker ranker;

    public RankerQuery(Query[] queries, Feature[] features, LtrRanker ranker) {
        assert queries.length == features.length;
        this.queries = queries;
        this.features = features;
        this.ranker = ranker;
    }

    public static RankerQuery build(LtrRanker ranker, Feature[] features, QueryShardContext context, Map<String, Object> params) {
        assert features.length >= ranker.size();
        Query[] queries = new Query[features.length];
        for(int i = 0; i < features.length; i++) {
            queries[i] = features[i].doToQuery(context, params);
        }
        return new RankerQuery(queries, features, ranker);
    }

    public static RankerQuery build(LtrRanker ranker, PrebuiltFeature[] features) {
        assert features.length >= ranker.size();
        Query[] queries = new Query[features.length];
        for(int i = 0; i < features.length; i++) {
            queries[i] = features[i].getPrebuiltQuery();
        }
        return new RankerQuery(queries, features, ranker);
    }

    /**
     * Expert: called to re-write queries into primitive queries. For example,
     * a PrefixQuery will be rewritten into a BooleanQuery that consists
     * of TermQuerys.
     *
     * @param reader
     */
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query[] rewrittenQueries = new Query[queries.length];
        boolean rewritten = false;
        for(int i = 0; i < queries.length; i++) {
            rewrittenQueries[i] = queries[i].rewrite(reader);
            rewritten |= rewrittenQueries[i] != queries[i];
        }
        return rewritten ? new RankerQuery(rewrittenQueries, features, ranker) : this;
    }

    public Feature getFeature(int idx) {
        return features[idx];
    }

    /**
     * Override and implement query instance equivalence properly in a subclass.
     * This is required so that {@link org.apache.lucene.search.QueryCache} works properly.
     * <p>
     * Typically a query will be equal to another only if it's an instance of
     * the same class and its document-filtering properties are identical that other
     * instance. Utility methods are provided for certain repetitive code.
     *
     * @param obj
     * @see #sameClassAs(Object)
     * @see #classHash()
     */
    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof RankerQuery)) {
            return false;
        }
        RankerQuery other = (RankerQuery) obj;
        return Arrays.deepEquals(queries, ((RankerQuery) obj).queries)
                && Arrays.deepEquals(features, ((RankerQuery) obj).features)
                && Objects.equals(ranker, other.ranker);
    }

    /**
     * Override and implement query hash code properly in a subclass.
     * This is required so that {@link org.apache.lucene.search.QueryCache} works properly.
     *
     * @see #equals(Object)
     */
    @Override
    public int hashCode() {
        return Objects.hash(features, queries, ranker);
    }

    /**
     * Prints a query to a string, with <code>field</code> assumed to be the
     * default field and omitted.
     *
     * @param field
     */
    @Override
    public String toString(String field) {
        return "rankerquery:"+field;
    }

    /**
     * Expert: Constructs an appropriate Weight implementation for this query.
     * <p>
     * Only implemented by primitive queries, which re-write to themselves.
     *
     * @param searcher
     * @param needsScores True if document scores ({@link Scorer#score}) or match
     *                    frequencies ({@link Scorer#freq}) are needed.
     */
    @Override
    public RankerWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        Weight[] weights = new Weight[queries.length];
        for(int i = 0; i < weights.length; i++) {
            weights[i] = searcher.createWeight(queries[i], needsScores);
        }
        return new RankerWeight(weights);
    }

    public class RankerWeight extends Weight {
        private final Weight[] weights;

        /**
         * Sole constructor, typically invoked by sub-classes.
         *
         * @param weights
         */
        protected RankerWeight(Weight[] weights) {
            super(RankerQuery.this);
            this.weights = weights;
        }

        /**
         * Expert: adds all terms occurring in this query to the terms set. If the
         * {@link Weight} was created with {@code needsScores == true} then this
         * method will only extract terms which are used for scoring, otherwise it
         * will extract all terms which are used for matching.
         *
         * @param terms
         */
        @Override
        public void extractTerms(Set<Term> terms) {
            for (Weight w : weights) {
                w.extractTerms(terms);
            }
        }

        /**
         * An explanation of the score computation for the named document.
         *
         * @param context the readers context to create the {@link Explanation} for.
         * @param doc     the document's id relative to the given context's reader
         * @return an Explanation for the score
         * @throws IOException if an {@link IOException} occurs
         */
        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            List<Explanation> subs = new ArrayList<>(weights.length);

            LtrRanker.DataPoint d = ranker.newDataPoint();
            for (int i = 0; i < weights.length; i++) {
                Weight weight = weights[i];
                Explanation explain = weight.explain(context, doc);
                String featureString = "Feature " + Integer.toString(i);
                if (features[i].getName() != null) {
                    featureString += "(" + features[i].getName() + ")";
                }
                featureString += ":";
                float featureVal = 0.0f;
                if (!explain.isMatch()) {
                    subs.add(Explanation.noMatch(featureString + " [no match, default value 0.0 used]"));
                }
                else {
                    subs.add(Explanation.match(explain.getValue(), featureString, explain));
                    featureVal = explain.getValue();
                }
                d.setFeatureScore(i, featureVal);
            }
            float modelScore = ranker.score(d);
            return Explanation.match(modelScore, " Model: " + ranker.name() + " using features:", subs);
        }

        /**
         * The value for normalization of contained query clauses (e.g. sum of squared weights).
         */
        @Override
        public float getValueForNormalization() throws IOException {
            float sum = 0.0f;
            for (Weight w : weights) {
                sum += w.getValueForNormalization();
            }
            return sum ;
        }

        /**
         * Assigns the query normalization factor and boost to this.
         *
         * @param norm
         * @param boost
         */
        @Override
        public void normalize(float norm, float boost) {
            for(Weight w : weights) {
                w.normalize(norm, boost);
            }
        }

        /**
         * Returns a {@link Scorer} which can iterate in order over all matching
         * documents and assign them a score.
         * <p>
         * <b>NOTE:</b> null can be returned if no documents will be scored by this
         * query.
         * <p>
         * <b>NOTE</b>: The returned {@link Scorer} does not have
         * {@link org.apache.lucene.index.LeafReader#getLiveDocs()} applied, they need to be checked on top.
         *
         * @param context the {@link LeafReaderContext} for which to return the {@link Scorer}.
         * @return a {@link Scorer} which scores documents in/out-of order.
         * @throws IOException if there is a low-level I/O error
         */
        @Override
        public RankerScorer scorer(LeafReaderContext context) throws IOException {
            RankerChildScorer[] scorers = new RankerChildScorer[weights.length];
            DocIdSetIterator[] subIterators = new DocIdSetIterator[weights.length];
            for(int i = 0; i < weights.length; i++) {
                Scorer scorer = weights[i].scorer(context);
                if (scorer == null) {
                    scorer = new NoopScorer(this, context.reader().maxDoc());
                }
                scorers[i] = new RankerChildScorer(scorer, features[i]);
                subIterators[i] = scorer.iterator();
            }
            DocIdSetIterator rankerIterator = new NaiveDisjunctionDISI(DocIdSetIterator.all(context.reader().maxDoc()), subIterators);
            return new RankerScorer(scorers, rankerIterator);
        }

        class RankerScorer extends Scorer {
            private final List<ChildScorer> scorers;
            private final DocIdSetIterator iterator;
            private final float[] scores;
            private final LtrRanker.DataPoint dataPoint;

            public RankerScorer(RankerChildScorer[] scorers, DocIdSetIterator iterator) {
                super(RankerWeight.this);
                this.scorers = Arrays.asList(scorers);
                scores = new float[scorers.length];
                this.iterator = iterator;
                dataPoint = ranker.newDataPoint();
            }

            /**
             * Returns the doc ID that is currently being scored.
             * This will return {@code -1} if the {@link #iterator()} is not positioned
             * or {@link DocIdSetIterator#NO_MORE_DOCS} if it has been entirely consumed.
             *
             * @see DocIdSetIterator#docID()
             */
            @Override
            public int docID() {
                return iterator.docID();
            }

            /**
             * Returns child sub-scorers
             *
             * @lucene.experimental
             */
            @Override
            public Collection<ChildScorer> getChildren() {
                return scorers;
            }

            /**
             * Returns the score of the current document matching the query.
             * Initially invalid, until {@link DocIdSetIterator#nextDoc()} or
             * {@link DocIdSetIterator#advance(int)} is called on the {@link #iterator()}
             * the first time, or when called from within {@link org.apache.lucene.search.LeafCollector#collect}.
             */
            @Override
            public float score() throws IOException {
                for(int i = 0; i < scorers.size(); i++) {
                    Scorer scorer = scorers.get(i).child;
                    if(scorer.docID() == docID()) {
                        dataPoint.setFeatureScore(i, scorer.score());
                    } else {
                        dataPoint.setFeatureScore(i, 0);
                    }
                }
                return ranker.score(dataPoint);
            }

            /**
             * Returns the freq of this Scorer on the current document
             */
            @Override
            public int freq() throws IOException {
                return scores.length;
            }

            /**
             * Return a {@link DocIdSetIterator} over matching documents.
             * <p>
             * The returned iterator will either be positioned on {@code -1} if no
             * documents have been scored yet, {@link DocIdSetIterator#NO_MORE_DOCS}
             * if all documents have been scored already, or the last document id that
             * has been scored otherwise.
             * <p>
             * The returned iterator is a view: calling this method several times will
             * return iterators that have the same state.
             */
            @Override
            public DocIdSetIterator iterator() {
                return iterator;
            }
        }
    }

    static class RankerChildScorer extends Scorer.ChildScorer {
        private final Feature feature;

        RankerChildScorer(Scorer scorer, Feature feature) {
            super(scorer, feature.getName());
            this.feature = feature;
        }
    }

    /**
     * Driven by a main iterator and tries to maintain a list of sub iterators
     */
    static class NaiveDisjunctionDISI extends DocIdSetIterator {
        private final DocIdSetIterator main;
        private final DocIdSetIterator[] subIterators;

        NaiveDisjunctionDISI(DocIdSetIterator main, DocIdSetIterator[] subIterators) {
            this.main = main;
            this.subIterators = subIterators;
        }

        /**
         * Returns the following:
         * <ul>
         * <li><code>-1</code> if {@link #nextDoc()} or
         * {@link #advance(int)} were not called yet.
         * <li>{@link #NO_MORE_DOCS} if the iterator has exhausted.
         * <li>Otherwise it should return the doc ID it is currently on.
         * </ul>
         * <p>
         *
         * @since 2.9
         */
        @Override
        public int docID() {
            return main.docID();
        }

        /**
         * Advances to the next document in the set and returns the doc it is
         * currently on, or {@link #NO_MORE_DOCS} if there are no more docs in the
         * set.<br>
         * <p>
         * <b>NOTE:</b> after the iterator has exhausted you should not call this
         * method, as it may result in unpredicted behavior.
         *
         * @since 2.9
         */
        @Override
        public int nextDoc() throws IOException {
            int doc = main.nextDoc();
            advanceSubIterators(doc);
            return doc;
        }

        /**
         * Advances to the first beyond the current whose document number is greater
         * than or equal to <i>target</i>, and returns the document number itself.
         * Exhausts the iterator and returns {@link #NO_MORE_DOCS} if <i>target</i>
         * is greater than the highest document number in the set.
         * <p>
         * The behavior of this method is <b>undefined</b> when called with
         * <code> target &le; current</code>, or after the iterator has exhausted.
         * Both cases may result in unpredicted behavior.
         * <p>
         * When <code> target &gt; current</code> it behaves as if written:
         * <p>
         * <pre class="prettyprint">
         * int advance(int target) {
         * int doc;
         * while ((doc = nextDoc()) &lt; target) {
         * }
         * return doc;
         * }
         * </pre>
         * <p>
         * Some implementations are considerably more efficient than that.
         * <p>
         * <b>NOTE:</b> this method may be called with {@link #NO_MORE_DOCS} for
         * efficiency by some Scorers. If your implementation cannot efficiently
         * determine that it should exhaust, it is recommended that you check for that
         * value in each call to this method.
         * <p>
         *
         * @param target
         * @since 2.9
         */
        @Override
        public int advance(int target) throws IOException {
            int docId = main.advance(target);
            advanceSubIterators(docId);
            return docId;
        }

        private void advanceSubIterators(int target) throws IOException {
            if (target == NO_MORE_DOCS) {
                return;
            }
            for(int i = 0; i < subIterators.length; i++) {
                DocIdSetIterator iterator = subIterators[i];
                if (iterator.docID() < target) {
                    iterator.advance(target);
                }
            }
        }

        /**
         * Returns the estimated cost of this {@link DocIdSetIterator}.
         * <p>
         * This is generally an upper bound of the number of documents this iterator
         * might match, but may be a rough heuristic, hardcoded value, or otherwise
         * completely inaccurate.
         */
        @Override
        public long cost() {
            return main.cost();
        }
    }
}
