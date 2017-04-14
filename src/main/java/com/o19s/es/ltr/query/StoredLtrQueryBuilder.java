package com.o19s.es.ltr.query;

import com.o19s.es.ltr.model.Feature;
import com.o19s.es.ltr.model.FeatureSet;
import com.o19s.es.ltr.model.FeatureStoreFactory;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StoredLtrQueryBuilder extends AbstractQueryBuilder<StoredLtrQueryBuilder> {
    private Map<String, Object> params;
    private String featureSet;
    private String featureStore;
    private String model;

    public static final String NAME = "sltr";
    private static final ObjectParser<StoredLtrQueryBuilder, QueryParseContext> PARSER;
    private static final ParseField FEATURE_SET = new ParseField("feature_set");
    private static final ParseField FEATURE_STORE = new ParseField("feature_store");
    private static final ParseField MODEL = new ParseField("model");
    private static final ParseField PARAMS = new ParseField("params");


    static {
        PARSER = new ObjectParser<StoredLtrQueryBuilder, QueryParseContext>(NAME, StoredLtrQueryBuilder::new);
        PARSER.declareObject(
                (ltr, params) -> ltr.params(params),
                (parser, context) -> parser.mapOrdered(),
                PARAMS
                );
        PARSER.declareString(
                (ltr, str) -> ltr.featureSet(str),
                FEATURE_SET
        );
        PARSER.declareString(
                (ltr, str) -> ltr.featureStore(str),
                FEATURE_STORE
        );
        PARSER.declareString(
                (ltr, str) -> ltr.model(str),
                MODEL
        );
        declareStandardFields(PARSER);
    }

    public StoredLtrQueryBuilder() {
    }

    public StoredLtrQueryBuilder(StreamInput in) throws IOException {
        super(in);
        params = in.readMap();
        featureSet = in.readString();
        featureStore = in.readString();
        model = in.readString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeMap(params);
        out.writeString(featureSet);
        out.writeString(featureStore);
        out.writeString(model);
    }

    public static StoredLtrQueryBuilder fromXContent(QueryParseContext context) {
        final StoredLtrQueryBuilder builder;
        try {
            builder = PARSER.apply(context.parser(), context);
        } catch(IllegalArgumentException iae) {
            throw new ParsingException(context.parser().getTokenLocation(), iae.getMessage());
        }
        if (builder.featureStore() == null) {
            throw new ParsingException(context.parser().getTokenLocation(), "[featureStore] must be set");
        }
        if (builder.featureSet() == null) {
            throw new ParsingException(context.parser().getTokenLocation(), "[featureSet] must be set");
        }
        if (builder.params() == null) {
            throw new ParsingException(context.parser().getTokenLocation(), "[params] must be set");
        }
        if (builder.model() == null) {
            throw new ParsingException(context.parser().getTokenLocation(), "[model] must be set");
        }
        return builder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FEATURE_SET.getPreferredName(), featureSet);
        builder.field(FEATURE_STORE.getPreferredName(), featureStore);
        builder.field(MODEL.getPreferredName(), model);
        builder.field(PARAMS.getPreferredName(), this.params);
        printBoostAndQueryName(builder);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        FeatureSet set = FeatureStoreFactory.load(context, featureStore).loadSet(featureSet);
        List<Query> queries = set.toQueries(context, params);
        List<String> featureNames = new ArrayList<>(queries.size());
        for(Feature f : set) {
            featureNames.add(f.getName());

        }
        Script script = new Script(ScriptType.FILE, "ranklib", model, Collections.emptyMap());
        RankLibScriptEngine.RankLibExecutableScript rankerScript =
                (RankLibScriptEngine.RankLibExecutableScript) context.getExecutableScript(script, ScriptContext.Standard.SEARCH);
        return new LtrQuery(queries, rankerScript._ranker, featureNames);
    }

    /**
     * Indicates whether some other {@link QueryBuilder} object of the same type is "equal to" this one.
     *
     * @param other
     */
    @Override
    protected boolean doEquals(StoredLtrQueryBuilder other) {
        return Objects.equals(params, other.params) &&
                Objects.equals(featureSet, other.featureSet) &&
                Objects.equals(featureStore, other.featureStore) &&
                Objects.equals(model, other.model);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(params, featureSet, featureStore, model);
    }

    /**
     * Returns the name of the writeable object
     */
    @Override
    public String getWriteableName() {
        return NAME;
    }

    public Map<String, Object> params() {
        return params;
    }

    public StoredLtrQueryBuilder params(Map<String, Object> params) {
        this.params = Objects.requireNonNull(params);
        return this;
    }

    public String featureSet() {
        return featureSet;
    }

    public StoredLtrQueryBuilder featureSet(String featureSet) {
        this.featureSet = Objects.requireNonNull(featureSet);
        return this;
    }

    public String featureStore() {
        return featureStore;
    }

    public StoredLtrQueryBuilder featureStore(String featureStore) {
        this.featureStore = Objects.requireNonNull(featureStore);
        return this;
    }

    public String model() {
        return model;
    }

    public StoredLtrQueryBuilder model(String model) {
        this.model = Objects.requireNonNull(model);
        return this;
    }
}
