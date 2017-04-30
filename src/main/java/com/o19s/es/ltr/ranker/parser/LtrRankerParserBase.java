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

import org.elasticsearch.index.query.QueryShardContext;

/**
 * Contextualized LtrRanker parser base.
 * Useful for model parsers that needs access to QueryShardContext
 * for i.e. accessing the {@link org.elasticsearch.script.ScriptService})
 */
public abstract class LtrRankerParserBase implements LtrRankerParser {
    protected final QueryShardContext context;

    protected LtrRankerParserBase(QueryShardContext context) {
        this.context = context;
    }
}