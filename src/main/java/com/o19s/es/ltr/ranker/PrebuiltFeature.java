package com.o19s.es.ltr.ranker;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryShardContext;

import java.util.Map;
import java.util.Objects;

public class PrebuiltFeature implements Feature {
    private final String name;
    private final Query query;

    public PrebuiltFeature(String name, Query query) {
        this.name = name;
        this.query = Objects.requireNonNull(query);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Query doToQuery(QueryShardContext context, Map<String, Object> params) {
        return query;
    }

    public Query getPrebuiltQuery() {
        return query;
    }
}
