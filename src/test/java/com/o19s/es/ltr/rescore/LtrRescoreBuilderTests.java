package com.o19s.es.ltr.rescore;

import com.o19s.es.ltr.feature.PrebuiltFeatureSet;
import com.o19s.es.ltr.feature.store.CompiledLtrModel;
import com.o19s.es.ltr.feature.store.MemStore;
import com.o19s.es.ltr.query.Normalizer;
import com.o19s.es.ltr.query.StoredLtrQueryBuilder;
import com.o19s.es.ltr.ranker.linear.LinearRanker;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

public class LtrRescoreBuilderTests extends AbstractBuilderTestCase {
    private NamedXContentRegistry registry;
    private NamedWriteableRegistry bregistry;
    private MemStore store = new MemStore("mem");

    @Before
    public void init() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(Normalizer.getNamedXContent());
        entries.add(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(StoredLtrQueryBuilder.NAME),
                (p) -> StoredLtrQueryBuilder.fromXContent((s,c) -> this.store, p)));
        registry = new NamedXContentRegistry(entries);
        List<NamedWriteableRegistry.Entry> bentries = new ArrayList<>(Normalizer.getNamedWriteables());
        bentries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, StoredLtrQueryBuilder.NAME,
                (stream) -> new StoredLtrQueryBuilder((s,c) -> this.store, stream)));
        bregistry = new NamedWriteableRegistry(bentries);
        store.add(new CompiledLtrModel("foo", new PrebuiltFeatureSet("set", new ArrayList<>()), new LinearRanker(new float[0])));
    }

    public void testToContext() throws IOException {
        Supplier<LtrRescoreBuilder> supplier = () -> new LtrRescoreBuilder()
                .setQuery(new StoredLtrQueryBuilder((s, c) -> this.store).modelName("foo").params(new HashMap<>()))
                .setQueryNormalizer(Normalizer.NOOP)
                .setRescoreQueryNormalizer(new Normalizer.Logistic(1F, 2F))
                .setQueryWeight(0.3F)
                .setRescoreQueryWeight(0.4F)
                .setScoreMode(LtrRescorer.LtrRescoreMode.Avg)
                .setScoringBatchSize(4)
                .windowSize(25);
        LtrRescorer.LtrRescoreContext context = (LtrRescorer.LtrRescoreContext) supplier.get().buildContext(createShardContext());
        assertEquals(Normalizer.NOOP, context.getQueryNormalizer());
        assertEquals(new Normalizer.Logistic(1F, 2F), context.getRescoreQueryNormalizer());
        assertEquals(0.3F, context.getQueryWeight(), Math.ulp(0.3F));
        assertEquals(0.4F, context.getRescoreQueryWeight(), Math.ulp(0.4F));
        assertEquals(LtrRescorer.LtrRescoreMode.Avg, context.getScoreMode());
        assertEquals(4, context.getBatchSize());
        assertEquals(25, context.getWindowSize());
        assertEquals("linear", context.getRankerQuery().getRanker().name());
    }

    public void testSerialization() throws IOException {
        Supplier<LtrRescoreBuilder> supplier = () -> new LtrRescoreBuilder()
                .setQuery(new StoredLtrQueryBuilder((s, c) -> this.store).modelName("foo").params(new HashMap<>()))
                .setQueryNormalizer(Normalizer.NOOP)
                .setRescoreQueryNormalizer(Normalizer.NOOP)
                .setQueryWeight(0.3F)
                .setRescoreQueryWeight(0.4F)
                .setScoreMode(LtrRescorer.LtrRescoreMode.Avg)
                .setScoringBatchSize(4)
                .windowSize(25);
        BytesStreamOutput output = new BytesStreamOutput();
        LtrRescoreBuilder original = supplier.get();
        original.writeTo(output);
        LtrRescoreBuilder builder = new LtrRescoreBuilder(new NamedWriteableAwareStreamInput(output.bytes().streamInput(), bregistry));
        assertEquals(original, builder);
    }


    public void testDefaults() throws IOException {
        String json = "{" +
                "\"ltr_query\": {\"sltr\": {\"model\":\"foo\", \"params\": {}}}" +
                "}";
        LtrRescoreBuilder builder = parse(json);
        assertEquals(Normalizer.NOOP, builder.getQueryNormalizer());
        assertEquals(Normalizer.NOOP, builder.getRescoreQueryNormalizer());
        assertEquals(1F, builder.getQueryWeight(), Math.ulp(1F));
        assertEquals(1F, builder.getRescoreQueryWeight(), Math.ulp(1F));
        assertEquals(-1, builder.getScoringBatchSize());
        assertEquals(LtrRescorer.LtrRescoreMode.Total, builder.getScoreMode());
        assertEquals(new StoredLtrQueryBuilder((s, c) -> this.store).modelName("foo").params(new HashMap<>()),
                builder.getQuery());
        assertNull(builder.windowSize());
    }

    public void testParse() throws IOException {
        String json = "{" +
                "\"query_normalizer\": {\"minmax\":{\"min\": -0.5, \"max\":0.5}}," +
                "\"rescore_query_normalizer\": {\"saturation\":{\"k\": 2.3, \"a\":0.8}}," +
                "\"query_weight\": 2.3," +
                "\"rescore_query_weight\": 2.5," +
                "\"ltr_query\": {\"sltr\": {\"model\":\"foo\", \"params\": {}}}," +
                "\"scoring_batch_size\": 23," +
                "\"score_mode\": \"avg\"" +
                "}";
        LtrRescoreBuilder builder = parse(json);
        assertEquals(new Normalizer.MinMax(-0.5F, 0.5F), builder.getQueryNormalizer());
        assertEquals(new Normalizer.Saturation(2.3F, 0.8F), builder.getRescoreQueryNormalizer());
        assertEquals(2.3F, builder.getQueryWeight(), Math.ulp(2.3F));
        assertEquals(2.5F, builder.getRescoreQueryWeight(), Math.ulp(2.5F));
        assertEquals(23, builder.getScoringBatchSize());
        assertEquals(LtrRescorer.LtrRescoreMode.Avg, builder.getScoreMode());
        assertEquals(new StoredLtrQueryBuilder((s, c) -> this.store).modelName("foo").params(new HashMap<>()),
                builder.getQuery());
        LtrRescoreBuilder reparsed = parse(Strings.toString(builder));
        assertEquals(builder, reparsed);
    }

    public void testEquals() {
        Supplier<LtrRescoreBuilder> supplier = () -> new LtrRescoreBuilder()
                .setQuery(new StoredLtrQueryBuilder((s, c) -> this.store).modelName("foo").params(new HashMap<>()))
                .setQueryNormalizer(Normalizer.NOOP)
                .setRescoreQueryNormalizer(Normalizer.NOOP)
                .setQueryWeight(0.3F)
                .setRescoreQueryWeight(0.4F)
                .setScoreMode(LtrRescorer.LtrRescoreMode.Avg)
                .setScoringBatchSize(4)
                .windowSize(12);

        assertEquals(supplier.get(), supplier.get());
        assertEquals(supplier.get().hashCode(), supplier.get().hashCode());

        assertNotEquals(supplier.get()
                .setQuery(new StoredLtrQueryBuilder((s, c) -> this.store)
                        .modelName("moo").params(new HashMap<>())), supplier.get());
        assertNotEquals(supplier.get().setQueryNormalizer(new Normalizer.Saturation(1F, 2F)), supplier.get());
        assertNotEquals(supplier.get().setRescoreQueryNormalizer(new Normalizer.Saturation(1F, 2F)), supplier.get());
        assertNotEquals(supplier.get().setQueryWeight(1F), supplier.get());
        assertNotEquals(supplier.get().setRescoreQueryWeight(1F), supplier.get());
        assertNotEquals(supplier.get().setScoreMode(LtrRescorer.LtrRescoreMode.Total), supplier.get());
        assertNotEquals(supplier.get().setScoringBatchSize(1), supplier.get());
        assertNotEquals(supplier.get().windowSize(21), supplier.get());
    }

    private LtrRescoreBuilder parse(String json) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent
                .createParser(registry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        return LtrRescoreBuilder.parse(parser);
    }
}