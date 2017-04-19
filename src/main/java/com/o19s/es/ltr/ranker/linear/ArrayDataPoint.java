package com.o19s.es.ltr.ranker.linear;

import com.o19s.es.ltr.ranker.LtrRanker;

/**
 * Simple array-backed datapoint
 */
public class ArrayDataPoint implements LtrRanker.DataPoint {
    final float[] scores;

    public ArrayDataPoint(int size) {
        this.scores = new float[size];
    }

    /**
     * Set the feature score.
     *
     * @param featureIdx
     * @param score
     */
    @Override
    public void setFeatureScore(int featureIdx, float score) {
        scores[featureIdx] = score;
    }

    /**
     * Get the feature score
     *
     * @param featureIdx
     * @return the score
     */
    @Override
    public float getFeatureScore(int featureIdx) {
        return scores[featureIdx];
    }
}
