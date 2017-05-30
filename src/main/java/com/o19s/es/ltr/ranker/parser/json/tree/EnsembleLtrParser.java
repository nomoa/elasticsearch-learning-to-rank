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
import com.o19s.es.ltr.ranker.LtrRanker;
import com.o19s.es.ltr.ranker.dectree.NaiveAdditiveDecisionTree;
import com.o19s.es.ltr.ranker.parser.LtrRankerParser;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

/**
 * Created by doug on 5/26/17.
 */
public class EnsembleLtrParser implements LtrRankerParser {
    public static final String NAME = "model/ensemble-parser+json";

    // Parse
    // {
    //    "ensemble": [
    //       {
    //         "weight": 0.5,
    //         "id": "1"
    //       "split": {
    //            "threshold": 252,
    //            "feature": "foo",
    //            "lhs": {
    //               "output": 224.0
    //             }
    //             "rhs": {
    //                "split": { /* another tree */ }
    //           ]
    //          },
    //       {
    //          ...
    //       }
    //    ]
    // }

    @Override
    public LtrRanker parse(FeatureSet featureSet, String model) {
        final ParsedEnsemble ensemble;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, model)) {
            ensemble = ParsedEnsemble.PARSER.apply(parser, featureSet);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Cannot parse model", ioe);
        }
        NaiveAdditiveDecisionTree.Node[] trees = new NaiveAdditiveDecisionTree.Node[ensemble.getTrees().size()];
        float weights[] = new float[ensemble.getTrees().size()];
        int i = 0;
        for (ParsedTree tree : ensemble.getTrees()) {
            weights[i] = (float) tree.weight();
            trees[i] = toNode(tree.root(), featureSet);
            i++;
        }
        return new NaiveAdditiveDecisionTree(trees, weights, featureSet.size());
    }


    private NaiveAdditiveDecisionTree.Node toNode(ParsedSplit sp, FeatureSet featureSet) {
        if (sp.isLeaf()) {
            return new NaiveAdditiveDecisionTree.Leaf((float) sp.output());
        } else {
            // feature existence has been checked while parsing
            assert featureSet.hasFeature(sp.feature());
            int featureOrd = featureSet.featureOrdinal(sp.feature());

            return new NaiveAdditiveDecisionTree.Split(toNode(sp.lhs(), featureSet),
                    toNode(sp.rhs(), featureSet),
                    featureOrd,
                    (float) sp.threshold());
        }
    }


    private static class ParsedEnsemble {
        // Use the parser context to pass our featureSet along
        // We can then check feature existence while parsing
        // This is not strictly necessary here since we could check this
        // while converting the ParsedTree into the NaiveAdditiveDecisionTree
        private static final ObjectParser<ParsedEnsemble, FeatureSet> PARSER;

        static {
            PARSER = new ObjectParser<>(NAME, ParsedEnsemble::new);
            PARSER.declareObjectArray(
                    ParsedEnsemble::setTrees,
                    ParsedTree.PARSER,
                    new ParseField("ensemble")
            );
        }

        private List<ParsedTree> trees;

        public List<ParsedTree> getTrees() {
            return trees;
        }

        public void setTrees(List<ParsedTree> trees) {
            this.trees = trees;
        }
    }
}
