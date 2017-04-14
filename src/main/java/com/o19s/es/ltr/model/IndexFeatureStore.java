package com.o19s.es.ltr.model;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;

import java.io.IOException;

public class IndexFeatureStore implements FeatureStore {
    private static final String FEATURE_TYPE = "features";
    private static final String SET_TYPE = "featuresets";
    private final String index;
    private final NodeClient client;

    public IndexFeatureStore(String index, NodeClient client) {
        this.index = index;
        this.client = client;
    }

    @Override
    public StoredFeature load(String id) throws IOException {
        GetResponse gr = client.prepareGet(index, SET_TYPE, id).get();
        return StoredFeature.load(gr.getSourceAsBytes());
    }

    @Override
    public FeatureSet loadSet(String id) throws IOException {
        GetResponse gr = client.prepareGet(index, SET_TYPE, id).get();
        return FeatureSet.load(gr.getSourceAsBytes());
    }
}
