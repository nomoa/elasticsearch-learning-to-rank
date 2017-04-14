package com.o19s.es.ltr.model;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.Map;

public class StoredFeature implements Writeable {
    private String id;
    private String name;
    private String featureData;

    static ObjectParser<StoredFeature, Void> PARSER;

    static {
        PARSER = new ObjectParser<>("ltr_feature");
        PARSER.declareString((feat, value) -> feat.id = value,
                new ParseField("id"));
        PARSER.declareString((feat, value) -> feat.name = value,
                new ParseField("name"));
        PARSER.declareField((parser, feat, value) -> {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                try (XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType())) {
                    feat.featureData = builder.copyCurrentStructure(parser).bytes().utf8ToString();
                } catch (IOException e) {
                    throw new ParsingException(parser.getTokenLocation(), "Could not parse inline template", e);
                }
            } else {
                feat.featureData = parser.text();
            }
        }, new ParseField("feature"), ObjectParser.ValueType.OBJECT_OR_STRING);
    }

    private StoredFeature() {
    }

    public StoredFeature(StreamInput in) throws IOException {
        id = in.readString();
        name = in.readString();
        featureData = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(name);
        out.writeString(featureData);
    }

    public static StoredFeature load(byte[] bytes) throws IOException {
        StoredFeature feature = new StoredFeature();
        XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, bytes);
        PARSER.parse(parser, feature, null);
        return feature;
    }

    public Query doToQuery(QueryShardContext context, Map<String, Object> params) {
        ExecutableScript script = context.getExecutableScript(
                new Script(ScriptType.INLINE, "mustache", featureData, params), ScriptContext.Standard.SEARCH);
        BytesReference source = (BytesReference) script.run();
        try {
            XContentParser parser = JsonXContent.jsonXContent.createParser(context.getXContentRegistry(), source);
            return new QueryParseContext(parser).parseInnerQueryBuilder().get().toFilter(context);
        } catch (IOException e) {
            throw new QueryShardException(context, "Cannot create parse to parse feature", e);
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public String getFeatureData() {
        return featureData;
    }

    public void setFeatureData(String featureData) {
        this.featureData = featureData;
    }
}
