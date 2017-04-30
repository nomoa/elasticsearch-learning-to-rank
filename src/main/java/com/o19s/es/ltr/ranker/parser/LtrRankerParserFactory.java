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

package com.o19s.es.ltr.ranker.parser;

import com.o19s.es.ltr.ranker.ranklib.RanklibScriptParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * LtrModel parser registry
 */
public class LtrRankerParserFactory {
    private static final Map<String, LtrRankerParserSupplier> REGISTRY;

    static {
        Map<String, LtrRankerParserSpec> registry = new HashMap<>();
        for(LtrRankerParserSpec parser : RanklibScriptParser.parsers()) {
            registry.put(parser.type(), parser);
        }
        REGISTRY = Collections.unmodifiableMap(registry);
    }
    private final Map<String, LtrRankerParserSupplier> parsers;
    private final QueryShardContext context;

    public static LtrRankerParserFactory defaultFactory(QueryShardContext context) {
        // Ideally this should be context free but we need to support
        // Script based model where the ScriptService is required
        return new LtrRankerParserFactory(context, REGISTRY);
    }

    public LtrRankerParserFactory(QueryShardContext context, Map<String, LtrRankerParserSupplier> parsers) {
        this.context = context;
        this.parsers = parsers;
    }

    /**
     *
     * @param type type or content-type like string defining the model format
     * @return a model parser
     * @throws IllegalArgumentException if the type is not supported
     */
    public LtrRankerParser getParser(String type) {
        LtrRankerParserSupplier supplier = parsers.get(type);
        if (supplier == null) {
            throw new IllegalArgumentException("Unsupported LtrRanker format/type [" + type + "]");
        }
        return supplier.get(context);
    }

    public LtrRankerParser getParser(Script script) {
        return getParser(scriptContentType(script.getLang(), script.getType()));
    }

    /**
     * Helper method to transform a script language+ScriptType to a LtrParser type
     * Mostly used to support existing ranklib script engine
     */
    public static String scriptContentType(String lang, ScriptType type) {
        return "model/"+lang+"+"+type.getParseField().getPreferredName();
    }

    @FunctionalInterface
    public interface LtrRankerParserSupplier {
        LtrRankerParser get(QueryShardContext context);
    }

    public static class LtrRankerParserSpec implements LtrRankerParserSupplier {
        private final String type;
        private final LtrRankerParserSupplier parser;

        public LtrRankerParserSpec(String type, LtrRankerParserSupplier parser) {
            this.type = type;
            this.parser = parser;
        }

        public String type() {
            return type;
        }

        @Override
        public LtrRankerParser get(QueryShardContext context) {
            return parser.get(context);
        }
    }
}
