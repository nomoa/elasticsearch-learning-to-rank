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

import com.o19s.es.ltr.feature.store.StoredFeature;
import com.o19s.es.ltr.feature.store.StoredFeatureSet;
import com.o19s.es.ltr.feature.store.StoredLtrModel;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.Iterator;
import java.util.Objects;

public class Caches {
    private final Cache<CacheKey, StoredFeature> featureCache;
    private final Cache<CacheKey, StoredFeatureSet> featureSetCache;
    private final Cache<CacheKey, StoredLtrModel> modelCache;

    public Caches(Settings settings) {
        // TODO: use settings
        TimeValue expireAfterWrite = TimeValue.timeValueMinutes(10);
        TimeValue expireAfterAccess = TimeValue.timeValueMinutes(2);
        long maxWeight = Math.min(JvmInfo.jvmInfo().getMem().getHeapMax().getBytes()/10, RamUsageEstimator.ONE_MB*10);
        this.featureCache = CacheBuilder.<CacheKey, StoredFeature>builder()
                .setExpireAfterWrite(expireAfterWrite)
                .setExpireAfterAccess(expireAfterAccess)
                .setMaximumWeight(maxWeight)
                .weigher((s, w) -> w.ramBytesUsed())
                .build();
        this.featureSetCache = CacheBuilder.<CacheKey, StoredFeatureSet>builder()
                .setExpireAfterWrite(expireAfterWrite)
                .setExpireAfterAccess(expireAfterAccess)
                .weigher((s, w) -> w.ramBytesUsed())
                .setMaximumWeight(maxWeight)
                .build();
        this.modelCache = CacheBuilder.<CacheKey, StoredLtrModel>builder()
                .setExpireAfterWrite(expireAfterWrite)
                .setExpireAfterAccess(expireAfterAccess)
                .weigher((s, w) -> w.ramBytesUsed())
                .setMaximumWeight(maxWeight)
                .build();
    }

    public void evict(String index) {
        evict(index, featureCache);
        evict(index, featureSetCache);
        evict(index, modelCache);
    }

    private void evict(String index, Cache<CacheKey, ?> cache) {
        Iterator<CacheKey> ite = cache.keys().iterator();
        while(ite.hasNext()) {
            if(ite.next().index.equals(index)) {
                ite.remove();
            }
        }
    }

    public Cache<CacheKey, StoredFeature> featureCache() {
        return featureCache;
    }

    public Cache<CacheKey, StoredFeatureSet> featureSetCache() {
        return featureSetCache;
    }

    public Cache<CacheKey, StoredLtrModel> modelCache() {
        return modelCache;
    }


    public static class CacheKey {
        private final String index;
        private final String id;

        public CacheKey(String index, String id) {
            this.index = Objects.requireNonNull(index);
            this.id = Objects.requireNonNull(id);
        }

        public String getIndex() {
            return index;
        }

        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey) o;

            if (!index.equals(cacheKey.index)) return false;
            return id.equals(cacheKey.id);
        }

        @Override
        public int hashCode() {
            int result = index.hashCode();
            result = 31 * result + id.hashCode();
            return result;
        }
    }
}
