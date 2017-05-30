/*
 * Copyright [2017] OpenSource Connections
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.o19s.es.ltr.ranker.parser.json.tree;

import com.o19s.es.ltr.feature.FeatureSet;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;

/**
 * Created by doug on 5/29/17.
 */
public class ParsedTree {
    public static final String NAME = "tree-ltr-parser";
    static final ObjectParser<ParsedTree, FeatureSet> PARSER;

    static {
        PARSER = new ObjectParser<>(NAME, ParsedTree::new);
        PARSER.declareObject(ParsedTree::root,
                             ParsedSplit.PARSER::parse,
                             new ParseField("split"));

        PARSER.declareDouble(ParsedTree::weight,
                             new ParseField("weight"));

        PARSER.declareString(ParsedTree::id,
                             new ParseField("id"));

    }

    public void root(ParsedSplit split) {
        _root = split;
    }

    public ParsedSplit root() {
        return _root;
    }

    public void weight(double val) {
        _weight = val;
    }

    public double weight() {
        return _weight;
    }

    public void id(String id) {
        _id = id;
    }

    public String id() {
        return _id;
    }

    private ParsedSplit _root;
    private double _weight;
    private String _id;



}
