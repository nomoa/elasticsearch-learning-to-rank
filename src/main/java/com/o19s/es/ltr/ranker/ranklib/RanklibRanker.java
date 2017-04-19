package com.o19s.es.ltr.ranker.ranklib;

import ciir.umass.edu.learning.Ranker;
import com.o19s.es.ltr.ranker.LtrRanker;

public class RanklibRanker implements LtrRanker {
    private final Ranker ranker;

    public RanklibRanker(Ranker ranker) {
        this.ranker = ranker;
    }

    /**
     * Model name
     *
     * @return the name of the model
     */
    @Override
    public String name() {
        return ranker.name();
    }

    /**
     * data point implementation used by this ranker
     * A data point is used to store feature scores.
     * A single instance will be created for every Scorer.
     * The implementation must not be thread-safe.
     *
     * @return LtrRanker data point implementation
     */
    @Override
    public DenseProgramaticDataPoint newDataPoint() {
        // ranklib models are 1-based
        return new DenseProgramaticDataPoint(ranker.getFeatures().length+1);
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
        assert point instanceof DenseProgramaticDataPoint;
        return (float) ranker.eval((DenseProgramaticDataPoint) point);
    }

    /**
     * @return the number of features supported by this ranker
     */
    @Override
    public int size() {
        return ranker.getFeatures().length;
    }
}
