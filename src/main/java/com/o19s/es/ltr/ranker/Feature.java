package com.o19s.es.ltr.ranker;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryShardContext;

import java.util.Map;

/**
 * A feature that can be transformed into a lucene query
 */
public interface Feature {
    String getName();
    public Query doToQuery(QueryShardContext context, Map<String, Object> params);
}
