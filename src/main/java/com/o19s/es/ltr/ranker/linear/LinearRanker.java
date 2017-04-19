package com.o19s.es.ltr.ranker.linear;

import com.o19s.es.ltr.ranker.LtrRanker;

/**
 * Simple linear ranker that applies a dot product based
 * on the provided weights array.
 */
public class LinearRanker implements LtrRanker {
    private final float[] weights;

    public LinearRanker(float weight[]) {
        this.weights = weight;
    }

    /**
     * Model name
     *
     * @return the name of the model
     */
    @Override
    public String name() {
        return "linear";
    }

    /**
     * data point implementation used by this ranker
     * A data point is used to store feature scores.
     * A single instance will be created for every Scorer.
     * The implementation must not be thread-safe.
     *
     * @return LtrR data point implementation
     */
    @Override
    public DataPoint newDataPoint() {
        return new ArrayDataPoint(size());
    }

    /**
     * Score the data point.
     * At this point all feature scores are set.
     * features that did not match are set with a score to 0
     *
     * @param point the populated data point
     * @return the score
     */
    @Override
    public float score(DataPoint point) {
        assert point instanceof ArrayDataPoint;
        float[] scores = ((ArrayDataPoint) point).scores;
        float score = 0;
        for (int i = 0; i < weights.length; i++) {
            score += weights[i]*scores[i];
        }
        return score;
    }

    /**
     * @return the number of features supported by this ranker
     */
    @Override
    public int size() {
        return weights.length;
    }
}
