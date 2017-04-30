/*
 * Copyright [2017] Doug Turnbull, Wikimedia Foundation
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

package com.o19s.es.ltr.ranker.ranklib;

import ciir.umass.edu.learning.Ranker;
import com.o19s.es.ltr.feature.FeatureSet;
import com.o19s.es.ltr.ranker.LtrRanker;
import com.o19s.es.ltr.ranker.parser.LtrRankerParser;
import com.o19s.es.ltr.ranker.parser.LtrRankerParserBase;
import com.o19s.es.ltr.ranker.parser.LtrRankerParserFactory;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Load a ranklib model from a script file, mostly a wrapper around the
 * existing script that complies with the {@link LtrRankerParser} interface
 */
public class RanklibScriptParser extends LtrRankerParserBase {
    private final ScriptType type;

    public static List<LtrRankerParserFactory.LtrRankerParserSpec> parsers() {
        return Arrays.stream(ScriptType.values())
                .map(RanklibScriptParser::parser)
                .collect(Collectors.toList());
    }

    public static LtrRankerParserFactory.LtrRankerParserSpec parser(ScriptType type) {
        return new LtrRankerParserFactory.LtrRankerParserSpec(toType(type),
                (ctx) -> new RanklibScriptParser(ctx, type));
    }

    public RanklibScriptParser(QueryShardContext ctx, ScriptType type) {
        super(ctx);
        this.type = type;
    }

    @Override
    public LtrRanker parse(FeatureSet set, String model) {
        // XXX: no verification (the FeatureSet may not match the provided model)
        Script script = new Script(type, RankLibScriptEngine.NAME, model, Collections.emptyMap());
        RankLibScriptEngine.RankLibExecutableScript rankerScript =
                (RankLibScriptEngine.RankLibExecutableScript)context.getExecutableScript(script, ScriptContext.Standard.SEARCH);
        return new RanklibRanker((Ranker)rankerScript.run());
    }

    private static String toType(ScriptType type) {
        return LtrRankerParserFactory.scriptContentType(RankLibScriptEngine.NAME, type);
    }
}
