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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

/**
 * Created by doug on 5/26/17.
 */
public class ParsedSplit {
    public static final String NAME = "json-ltr-split-parser";
    public static final ObjectParser<ParsedSplit, FeatureSet> PARSER;


    static {
        PARSER = new ObjectParser<>(NAME, ParsedSplit::new);
        PARSER.declareString(ParsedSplit::featureName,
                new ParseField("feature"));
        PARSER.declareDouble(ParsedSplit::threshold,
                new ParseField("threshold"));
        PARSER.declareObject( ParsedSplit::lhs,
                PARSER::parse,
                new ParseField("lhs"));
        PARSER.declareObject( ParsedSplit::rhs,
                ParsedSplit::parse,
                new ParseField("rhs"));
        PARSER.declareObject( ParsedSplit::rhs,
                ParsedSplit::parse,
                new ParseField("rhs"));
        PARSER.declareDouble( ParsedSplit::output,
                new ParseField("output"));

    }

    public void output(double val) {_output = val;}

    public String featureName() {
        return _featureName;
    }

    public ParsedSplit lhs() {
        return _lhs;
    }

    public ParsedSplit rhs() {
        return _rhs;
    }

    public double threshold() {
        return _threshold;
    }

    public double output() {
        return _output;
    }

    public String feature() {
        return _featureName;
    }

    public boolean isLeaf() {return _output != null;}
    public boolean isSplit() {
        return _featureName != null || _rhs != null || _lhs != null || _threshold != null;
    }
    public boolean isValidSplit() {
        return _featureName != null && _rhs != null && _lhs != null && _threshold != null;
    }

    public void threshold(double val) {
        _threshold = val;
    }

    public void featureName(String name) {
        _featureName = name;
    }

    public void lhs(ParsedSplit split) { _lhs = Objects.requireNonNull(split); }

    public void rhs(ParsedSplit split)  { _rhs = Objects.requireNonNull(split); }


    public static ParsedSplit parse(XContentParser xParser, FeatureSet context) {
        ParsedSplit split = PARSER.apply(xParser, context);
        if (split.feature() != null && !context.hasFeature(split.feature())) {
            throw new ParsingException(xParser.getTokenLocation(), "Unknown feature [" + split.feature() + "]");
        }
        if (split.isLeaf() && split.isSplit()) {
            throw new ParsingException(xParser.getTokenLocation(), "Invalid split, [output] used with either [rhs], [lhs], [feature] or [threshold]");
        }
        if (split.isSplit() && !split.isValidSplit()) {
            throw new ParsingException(xParser.getTokenLocation(), "Invalid split missing [rhs], [lhs], [feature] or [threshold]");
        }
        return split;
    }

    private String _featureName;
    private Double _threshold;
    private Double _output;
    private ParsedSplit _lhs;
    private ParsedSplit _rhs;
    private boolean _isLeaf = false;
}
