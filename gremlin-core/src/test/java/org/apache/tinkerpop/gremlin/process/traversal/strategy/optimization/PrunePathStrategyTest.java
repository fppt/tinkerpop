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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Ted Wilmes (http://twilmes.org)
 */
@RunWith(Parameterized.class)
public class PrunePathStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal traversal;

    @Parameterized.Parameter(value = 1)
    public Set<String> labels;

    void applyPrunePathStrategy(final Traversal traversal) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(PrunePathStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();
    }

    @Test
    public void doTest() {
        applyPrunePathStrategy(traversal);
        assertEquals(labels, ((PathProcessor)traversal.asAdmin().getEndStep()).getKeepLabels());
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        return Arrays.asList(new Traversal[][]{
                {__.V().as("a").out().as("b").where(neq("a")).out(), }
//                {__.V().as("a").select("a"), Arrays.asList(1, 2, 3)}
//                {__.V().as("a").out().where(neq("a")), __.V().as("a").out().where(neq("a"))},
//                {__.V().match(__.as("a").out().as("b"), __.as("b").out().as("c")).select("b"), __.V().match(__.as("a").out().as("b"), __.as("b").out().as("c")).select("b")}
//                {__.V().as("a").out().as("b").out().where((neq("a"))).both().values("name"), __.V().as("a").out().out().where(neq("a")).prunePath(true, "a").both().values("name")},
//                {__.V().as("a").out().as("b").select("a", "b"), __.V().as("a").out().as("b").select("a", "b")},
//                {__.V().as("a").out().as("b").out().as("c").select("a", "b"), __.V().as("a").out().as("b").out().select("a", "b")},
//                {__.V().as("a").out().as("b").select("a"), __.V().as("a").out().select("a")},
//                {__.out().as("a").out().dedup("a").out(), __.out().as("a").out().dedup("a").prunePath(true, "a").out()}
        });
    }
}
