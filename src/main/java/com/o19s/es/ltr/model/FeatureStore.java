package com.o19s.es.ltr.model;

import java.io.IOException;

public interface FeatureStore {
    StoredFeature load(String id) throws IOException;
    FeatureSet loadSet(String id) throws IOException;
}