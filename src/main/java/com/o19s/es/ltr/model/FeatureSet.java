package com.o19s.es.ltr.model;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FeatureSet implements Writeable, Iterable<StoredFeature> {
    private String id;
    private String name;
    private List<StoredFeature> features;
    private static final ObjectParser<FeatureSet, Void> PARSER;

    static {
        PARSER = new ObjectParser<FeatureSet, Void>("feature_set");
        PARSER.declareString((feat, id) -> feat.id = id, new ParseField("id"));
        PARSER.declareString((feat, name) -> feat.name = name, new ParseField("name"));
        PARSER.declareObjectArray(
                (set, features) -> set.features = features,
                StoredFeature.PARSER,
                new ParseField("features")
                );
    }

    public FeatureSet() {
    }

    public List<Query> toQueries(QueryShardContext context, Map<String, Object> params) {
        List<Query> queries = new ArrayList<>(features.size());

        for(StoredFeature feature : features) {
            queries.add(feature.doToQuery(context, params));
        }
        return queries;
    }

    public FeatureSet(StreamInput in) throws IOException {
        id = in.readString();
        name = in.readString();
        features = in.readList(StoredFeature::new);
    }

    public static FeatureSet load(byte[] bytes) throws IOException {
        List<String> ids = new ArrayList<>();
        XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, bytes);
        return PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(name);
        out.writeList(features);
    }

    @Override
    public Iterator<StoredFeature> iterator() {
        return features.iterator();
    }
}
