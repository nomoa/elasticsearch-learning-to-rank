package com.o19s.es.ltr.model;

import org.elasticsearch.common.cache.Cache;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class CachedFeatureStore implements FeatureStore {
    private final FeatureStore store;
    private final CacheProvider cacheProvider;

    public CachedFeatureStore(FeatureStore store, CacheProvider cacheProvider) {
        this.store = store;
        this.cacheProvider = cacheProvider;
    }

    @Override
    public StoredFeature load(String id) throws IOException {
        try {
            return cacheProvider.featureCache(store).computeIfAbsent(id, (miss) -> store.load(miss));
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
    }

    @Override
    public FeatureSet loadSet(String id) throws IOException {
        try {
            return cacheProvider.featureSetCache(store).computeIfAbsent(id, (miss) -> store.loadSet(id));
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
    }

    public interface CacheProvider {
        Cache<String, StoredFeature> featureCache(FeatureStore store);
        Cache<String, FeatureSet> featureSetCache(FeatureStore store);
    }
}