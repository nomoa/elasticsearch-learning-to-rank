package com.o19s.es.ltr.ranker;

public interface LtrRanker {

    /**
     * Model name
     * @return the name of the model
     */
    String name();

    /**
     * data point implementation used by this ranker
     * A data point is used to store feature scores.
     * A single instance will be created for every Scorer.
     * The implementation must not be thread-safe.
     * @return LtrR data point implementation
     */
    DataPoint newDataPoint();

    /**
     * Score the data point.
     * At this point all feature scores are set.
     * features that did not match are set with a score to 0
     * @param point the populated data point
     * @return the score
     */
    float score(DataPoint point);

    /**
     * @return the number of features supported by this ranker
     */
    int size();

    /**
     * DataPoint
     */
    interface DataPoint {
        /**
         * Set the feature score.
         */
        void setFeatureScore(int featureIdx, float score);

        /**
         * Get the feature score
         * @param featureIdx
         * @return the score
         */
        float getFeatureScore(int featureIdx);
    }
}
