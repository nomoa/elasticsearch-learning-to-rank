/*
 * Copyright [2017] Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.o19s.es.ltr.feature.store.index;

import com.o19s.es.ltr.feature.Feature;
import com.o19s.es.ltr.feature.FeatureSet;
import com.o19s.es.ltr.feature.LtrModel;
import com.o19s.es.ltr.feature.store.FeatureStore;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.cache.Cache;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class CachedFeatureStore implements FeatureStore {
    private final IndexFeatureStore inner;
    private final Caches caches;

    public CachedFeatureStore(IndexFeatureStore inner, Caches caches) {
        this.inner = inner;
        this.caches = caches;
    }

    @Override
    public Feature load(String id) throws IOException {
        return innerLoad(id, caches.featureCache(), inner::load);
    }

    @Override
    public FeatureSet loadSet(String id) throws IOException {
        return innerLoad(id, caches.featureSetCache(), inner::loadSet);
    }

    @Override
    public LtrModel loadModel(String id) throws IOException {
        return innerLoad(id, caches.modelCache(), inner::loadModel);
    }

    private <T> T innerLoad(String id, Cache<Caches.CacheKey, T> cache, CheckedFunction<String, T, IOException> loader) throws IOException {
        try {
            return cache.computeIfAbsent(new Caches.CacheKey(inner.getIndex(), id), (k) -> loader.apply(k.getId()));
        } catch (ExecutionException e) {
            throw new IOException(e.getMessage(), e.getCause());
        }
    }
}
