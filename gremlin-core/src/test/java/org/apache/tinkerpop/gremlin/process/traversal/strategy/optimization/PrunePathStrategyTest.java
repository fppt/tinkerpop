/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Ted Wilmes (http://twilmes.org)
 */
@RunWith(Parameterized.class)
public class PrunePathStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin traversal;

    @Parameterized.Parameter(value = 1)
    public List<Set<String>> labels;

    void applyPrunePathStrategy(final Traversal traversal) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(PrunePathStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();
    }

    @Test
    public void doTest() {
        applyPrunePathStrategy(traversal);
        // get all path processors
        List<Set<String>> keepLabels = getKeepLabels(traversal);

        assertEquals(labels, keepLabels);
    }

    private List<Set<String>> getKeepLabels(Traversal.Admin traversal) {
        List<Set<String>> keepLabels = new ArrayList<>();
        for(Step step : (List<Step>)traversal.getSteps()) {
            if(step instanceof PathProcessor) {
                keepLabels.add(((PathProcessor) step).getKeepLabels());
            }
        }
        return keepLabels;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        return Arrays.asList(new Object[][]{
                {__.V().as("a").out().as("b").where(neq("a")).out(), Arrays.asList(Collections.EMPTY_SET)},
                {__.V().as("a").out().where(neq("a")).out().select("a"), Arrays.asList(Collections.singleton("a"), Collections.EMPTY_SET)},
                {__.V().match(__.as("a").out().as("b")), Arrays.asList(new HashSet<>(Arrays.asList("a", "b")))},
                // This test is a bit deceiving, a match will report back that it is keeping all of its start and end labels
                // to prevent child match traversals from dropping labels that are required by the parent.
                // In reality, the MatchStep will be dropping all starts and ends that are not required by latter
                // steps in the traversal.
                {__.V().match(__.as("a").out().as("b")).select("a"), Arrays.asList(new HashSet<>(Arrays.asList("a", "b")), Collections.EMPTY_SET)},
                {__.V().out().out().match(
                        as("a").in("created").as("b"),
                        as("b").in("knows").as("c")).select("c").out("created").where(neq("a")).values("name"),
                        Arrays.asList(new HashSet<>(Arrays.asList("a", "b", "c")), Collections.singleton("a"), Collections.EMPTY_SET)}
        });
    }
}
